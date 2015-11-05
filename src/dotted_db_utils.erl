-module(dotted_db_utils).

-compile(export_all).

-include("dotted_db.hrl").


make_request_id() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

-spec primary_node(bkey()) -> index_node().
primary_node(Key) ->
    DocIdx = riak_core_util:chash_key(Key),
    riak_core_apl:first_up(DocIdx, dotted_db).

-spec replica_nodes(bkey()) -> [index_node()].
replica_nodes(Key) ->
    case ets:lookup(?ETS_CACHE_REPLICA_NODES, Key) of
        []  -> % we still haven't cached this key replica nodes
            DocIdx = riak_core_util:chash_key(Key),
            IndexNodes = [IndexNode || {IndexNode, _Type} <- riak_core_apl:get_primary_apl(DocIdx, ?REPLICATION_FACTOR, dotted_db)],
            true = ets:insert(?ETS_CACHE_REPLICA_NODES, {Key, IndexNodes}),
            IndexNodes;
        [{_,IndexNodes}] ->
            IndexNodes
    end.

-spec replica_nodes_indices(bkey()) -> [index()].
replica_nodes_indices(Key) ->
    [Index || {Index,_Node} <- replica_nodes(Key)].


-spec random_index_node() -> [index_node()].
random_index_node() ->
    % getting the binary consistent hash is more efficient since it lives in a ETS.
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    IndexNodes = chashbin:to_list(RingBin),
    random_from_list(IndexNodes).

-spec vnodes_from_node(node()) -> [index_node()].
vnodes_from_node(TargetNode) ->
    % getting the binary consistent hash is more efficient since it lives in a ETS.
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    Filter = fun ({_Index, Owner}) -> Owner =:= TargetNode end,
    chashbin:to_list_filter(Filter, RingBin).

%% @doc Returns the nodes that also replicate a subset of keys from some node "NodeIndex".
%% We are assuming a consistent hashing ring, thus we return the N-1 before this node in the
%% ring and the next N-1.
-spec peers(index()) -> [index_node()].
peers(Index) ->
    peers(Index, ?REPLICATION_FACTOR).
-spec peers(index(), pos_integer()) -> [index_node()].
peers(Index, N) ->
    % {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    % getting the binary consistent hash is more efficient since it lives in a ETS.
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    Ring = chashbin:to_chash(RingBin),
    IndexBin = <<Index:160/integer>>,
    Indices = chash:successors(IndexBin, Ring),
    % PL = riak_core_ring:preflist(IndexBin, Ring),
    % Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.


%% @doc Returns a random element from a given list.
-spec random_from_list([any(),...]) -> any().
random_from_list(List) ->
    % properly seeding the process
    maybe_seed(),
    % get a random index withing the length of the list
    Index = random:uniform(length(List)),
    % return the element in that index
    lists:nth(Index,List).

%% @doc Returns a random element from a given list.
-spec random_sublist([any()], non_neg_integer()) -> [any()].
random_sublist(List, N) ->
    % Properly seeding the process.
    maybe_seed(),
    % Assign a random value for each element in the list.
    List1 = [{random:uniform(), E} || E <- List],
    % Sort by the random number.
    List2 = lists:sort(List1),
    % Take the first N elements.
    List3 = lists:sublist(List2, N),
    % Remove the random numbers.
    [ E || {_,E} <- List3].

-spec encode_kv(term()) -> binary().
encode_kv(Term) ->
    term_to_binary(Term).

-spec decode_kv(binary()) -> term().
decode_kv(Binary) ->
    binary_to_term(Binary).

-spec human_filesize(non_neg_integer() | float()) -> list().
human_filesize(Size) -> human_filesize(Size, ["B","KB","MB","GB","TB","PB"]).
human_filesize(S, [_|[_|_] = L]) when S >= 1024 -> human_filesize(S/1024, L);
human_filesize(S, [M|_]) ->
    lists:flatten(io_lib:format("~.2f ~s", [float(S), M])).


-spec maybe_seed() -> ok.
maybe_seed() ->
    case get(random_seed) of
        undefined ->
            <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
            random:seed({A,B,C}), ok;
        {X,X,X} ->
            <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
            random:seed({A,B,C}), ok;
        _ -> ok
    end.
