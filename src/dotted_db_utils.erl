-module(dotted_db_utils).

-compile(export_all).

-include("dotted_db.hrl").


make_request_id() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

-spec primary_node(key()) -> index_node().
primary_node(Key) ->
    DocIdx = get_DocIdx(Key),
    {IndexNode, _Type}  = riak_core_apl:first_up(DocIdx, dotted_db),
    IndexNode.

-spec replica_nodes(key()) -> [index_node()].
replica_nodes(Key) ->
    DocIdx = get_DocIdx(Key),
    [IndexNode || {IndexNode, _Type} <- riak_core_apl:get_primary_apl(DocIdx, ?N, dotted_db)].

get_DocIdx(Key) when is_binary(Key) ->
    riak_core_util:chash_key({?DEFAULT_BUCKET, Key}).
    

-spec replica_nodes_indices(key()) -> [index()].
replica_nodes_indices(Key) ->
    [Index || {Index,_Node} <- replica_nodes(Key)].

-spec get_node_from_index(index()) -> index_node().
get_node_from_index(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Node = riak_core_ring:index_owner(Ring, Index),
    {Index,Node}.

-spec random_index_node() -> [index_node()].
random_index_node() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexNodes = riak_core_ring:all_owners(Ring),
    random_from_list(IndexNodes).

% -spec random_index_from_node(node()) -> [index_node()].
% random_index_from_node(TargetNode) ->
%     {ok, Ring} = riak_core_ring_manager:get_my_ring(),
%     IndexNodes = [{Idx,Node} || {Idx,Node} <- riak_core_ring:all_owners(Ring), Node =:= TargetNode],
%     random_from_list(IndexNodes).

-spec random_index_from_node(node()) -> [index_node()].
random_index_from_node(TargetNode) ->
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    Filter = fun ({_Index, Owner}) -> Owner =:= TargetNode end,
    IndexNodes = chashbin:to_list_filter(Filter, RingBin),
    random_from_list(IndexNodes).


%% @doc Returns the nodes that also replicate a subset of keys from some node "NodeIndex".
%% We are assuming a consistent hashing ring, thus we return the N-1 before this node in the
%% ring and the next N-1.
-spec peers(index()) -> [index()].
peers(NodeIndex) ->
    peers(NodeIndex, ?N).
-spec peers(index(), pos_integer()) -> [index_node()].
peers(NodeIndex, N) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexBin = <<NodeIndex:160/integer>>,
    Indices = riak_core_ring:preflist(IndexBin, Ring),
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


-spec encode_kv(term()) -> binary().
encode_kv(Term) ->
    term_to_binary(Term).

-spec decode_kv(binary()) -> term().
decode_kv(Binary) ->
    binary_to_term(Binary).