-module(dotted_db_utils).

-compile(export_all).

-include("dotted_db.hrl").


make_request_id() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

-spec primary_node(key()) -> index_node().
primary_node(Key) ->
    DocIdx = riak_core_util:chash_key({?DEFAULT_BUCKET, Key}),
    {IndexNode, _Type}  = riak_core_apl:first_up(DocIdx, dotted_db),
    IndexNode.

-spec replica_nodes(key()) -> [index_node()].
replica_nodes(Key) ->
    DocIdx = riak_core_util:chash_key({?DEFAULT_BUCKET, Key}),
    [IndexNode || {IndexNode, _Type} <- riak_core_apl:get_primary_apl(DocIdx, ?REPLICATION_FACTOR, dotted_db)].

-spec replica_nodes_indices(key()) -> [index()].
replica_nodes_indices(Key) ->
    [Index || {Index,_Node} <- replica_nodes(Key)].

% -spec get_node_from_index(index()) -> index_node().
% get_node_from_index(Index) ->
%     {ok, Ring} = riak_core_ring_manager:get_my_ring(),
%     Node = riak_core_ring:index_owner(Ring, Index),
%     {Index,Node}.

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
-spec peers(index()) -> [index()].
peers(NodeIndex) ->
    peers(NodeIndex, ?REPLICATION_FACTOR).
-spec peers(index(), pos_integer()) -> [index_node()].
peers(NodeIndex, N) ->
    % {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    % getting the binary consistent hash is more efficient since it lives in a ETS.
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    Ring = chashbin:to_chash(RingBin),
    IndexBin = <<NodeIndex:160/integer>>,
    Indices = chash:successors(IndexBin, Ring),
    % PL = riak_core_ring:preflist(IndexBin, Ring),
    % Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.


%% @doc Returns a random element from a given list.
-spec random_from_list([any()]) -> any().
random_from_list(List) ->
    % properly seeding the process
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A,B,C}),
    % get a random index withing the length of the list
    Index = random:uniform(length(List)),
    % return the element in that index
    lists:nth(Index,List).

%% @doc Returns a random element from a given list.
-spec random_sublist([any()], integer()) -> [any()].
random_sublist(List, N) ->
    % Properly seeding the process.
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A,B,C}),
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

% pretty print stats
pp(M) ->
    #{
        nodeA                := A   ,
        nodeB                := B   ,
        % start_time           := ST  ,
        % ending_time          := ET  ,
        b2a_number           := OL  ,
        % b2a_size             := SOS ,
        % b2a_size_full        := FOS ,
        keylog_length_b      := KLL ,
        % keylog_size_b        := KLS ,
        % replicated_vv_size_b := VV  ,
        vv_b                 := VV2,
        kl_b                 := KL,
        bvv_b                := BVV
        % rem_entry            := RemoteEntry
        % peers                := Peers
    } = M,
    List = orddict:to_list(VV2),
    VV3 = lists:sort([Val || {_,Val} <- List]),
    List2 = orddict:to_list(BVV),
    BVV2 = lists:sort([Val || {_,Val} <- List2]),
    io:format("\n\n========== SYNC ==========   \n"),
    io:format("\t Node A:                       \t ~p\n",[A]),
    io:format("\t Node B:                       \t ~p\n",[B]),
    % io:format("\t Peers:                        \t ~p\n",[Peers]),
    % io:format("\t Duration (micro):             \t ~p\n",[ET-ST]),
    io:format("\t Objects Transfers:            \t ~p\n",[OL]),
    % io:format("\t Objects Size:                 \t ~p\n",[SOS]),
    % io:format("\t Objects Size Full:            \t ~p\n",[FOS]),
    io:format("\t KeyLog Length:                \t ~p\n",[KLL]),
    % io:format("\t KeyLog Size:                  \t ~p\n",[KLS]),
    % io:format("\t Replicated VV Size:           \t ~p\n",[VV]),
    io:format("\t KeyLog:                       \t ~p\n",[KL]),
    io:format("\t BVV:                          \t ~p\n",[BVV2]),
    io:format("\t VV:                           \t ~p\n\n",[VV3]).
    % io:format("\t Rem Entry:                    \t ~p\n",[RemoteEntry]).


% -spec get_time_sec() -> float().
% get_time_sec() ->
%     {Mega,Sec,Micro} = os:timestamp(),
%     % (Mega * 1000000) + Sec + (Micro / 1000000).
%     Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.


human_filesize(Size) -> human_filesize(Size, ["B","KB","MB","GB","TB","PB"]).
human_filesize(S, [_|[_|_] = L]) when S >= 1024 -> human_filesize(S/1024, L);
human_filesize(S, [M|_]) ->
    lists:flatten(io_lib:format("~.2f ~s", [float(S), M])).

