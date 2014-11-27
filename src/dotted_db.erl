-module(dotted_db).
-include("dotted_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile({no_auto_import,[put/2,get/1]}).

-export([
         ping/0,
         new_client/0,
         new_client/1,
         get/1,
         get/2,
         put/2,
         put/3,
         put_at_node/3,
         put_at_node/4,
         % delete/1,
         delete/2,
         % delete_at_node/2,
         delete_at_node/3,
         sync/0,
         sync_local/0,
         test/0,
         get_dbg_preflist/1,
         get_dbg_preflist/2
        ]).

-ignore_xref([
              ping/0
             ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, dotted_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, dotted_db_vnode_master).


new_client() ->
    new_client(node()).
new_client(Node) ->
    case net_adm:ping(Node) of
        pang -> {error, {could_not_reach_node, Node}};
        pong -> {ok, {?MODULE, Node}}
    end.

%% @doc Get a value from a key. If no target node is specified, use this node.
get(Key) ->
    {ok, LocalNode} = new_client(),
    get(Key, LocalNode).
get(Key, {?MODULE, TargetNode}) ->
    BinKey = dotted_db_utils:encode_kv(Key),
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, Me, BinKey],
    case node() of
        % if this node is already the target node
        TargetNode ->
            dotted_db_get_fsm_sup:start_get_fsm(Request);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, dotted_db_get_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).

%% @doc
put(Key, Value) ->
    put(Key, Value, vv:new()).
put(Key, Value, Context) ->
    {ok, LocalNode} = new_client(),
    put_del([Key, Value, Context, ?WRITE_OP], LocalNode).

put_at_node(Key, Value, TargetNode) ->
    put_at_node(Key, Value, vv:new(), TargetNode).
put_at_node(Key, Value, Context, TargetNode) ->
    put_del([Key, Value, Context, ?WRITE_OP], TargetNode).


% delete(Key) ->
%     delete(Key, vv:new()).
delete(Key, Context) ->
    {ok, LocalNode} = new_client(),
    put_del([Key, undefined, Context, ?DELETE_OP], LocalNode).

% delete_at_node(Key, TargetNode) ->
%     delete_at_node(Key, vv:new(), TargetNode);
delete_at_node(Key, Context, TargetNode) ->
    put_del([Key, undefined, Context, ?DELETE_OP], TargetNode).

% @doc Writes normal PUTs and DELETEs
put_del([Key, Value, Context, Operation], {?MODULE, TargetNode}) ->
    BinKey = dotted_db_utils:encode_kv(Key),
    BinValue = dotted_db_utils:encode_kv(Value),
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, Me, Operation, BinKey, BinValue, Context],
    case node() of
        TargetNode ->
            dotted_db_put_fsm_sup:start_put_fsm(Request);
        _ ->
            proc_lib:spawn_link(TargetNode, dotted_db_put_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).



%% @doc Forces a random anti-entropy synchronization with another node.
sync() ->
    IdxNode = dotted_db_utils:random_index_node(),
    do_sync(IdxNode).

%% @doc Forces a anti-entropy synchronization with a vnode from this node 
%% with another random vnode.
sync_local() ->
    ThisNode = node(),
    IdxNode = {_, ThisNode} = dotted_db_utils:random_local_index_node(),
    do_sync(IdxNode).

% @doc Private function used by sync/0 and sync_local/0.
do_sync(IdxNode = {_, TargetNode}) ->
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, Me, IdxNode],
    case node() of
        TargetNode ->
            dotted_db_sync_fsm_sup:start_sync_fsm(Request);
        _ ->
            proc_lib:spawn_link(TargetNode, dotted_db_sync_fsm, start_link, Request)
    end,
    {ok, Stats} = wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT),
    stats:pp(Stats).



test() ->
    {not_found, _} = get("random_key"),
    K1 = "key9",
    ok = put(K1,"v1"),
    ok = put("key2","vb"),
    ok = put(K1,"v3"),
    ok = put("key3","vc"),
    ok = put(K1,"v2"),
    {ok, {Values, Ctx}} = get(K1),
    V123 = lists:sort(Values),
    % ?PRINT(V123),
    ["v1","v2","v3"] = V123,
    ok = put(K1, "final", Ctx),
    {ok, {Final, Ctx2}} = get(K1),
    % ?PRINT(Final),
    ["final"] = Final,
    ok = delete(K1,Ctx2),
    Del = get(K1),
    % ?PRINT(Del),
    {not_found, _Ctx3} = Del,
    sync_local(),
    sync(),
    ok.




%%%===================================================================
%%% Internal Functions
%%%===================================================================

wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, error, Error}       -> {error, Error};
        % get
        {ReqID, not_found, Context} -> {not_found, Context};
        {ReqID, ok, Reply}          -> {ok, decode_get_reply(Reply)};
        % put/delete
        {ReqID, ok}                 -> ok;
        {ReqID, timeout}            -> {error, timeout}
    after Timeout ->
            {error, timeout}
    end.


decode_get_reply({sync, Stats}) ->
    Stats;
decode_get_reply({BinValues, Context}) ->
    Values = [ dotted_db_utils:decode_kv(BVal) || BVal <- BinValues ],
    {Values, Context}.











get_dbg_preflist(Key) ->
    DocIdx = riak_core_util:chash_key({<<"b">>,
                                       list_to_binary(Key)}),
    riak_core_apl:get_apl(DocIdx, ?N, dotted_db).

get_dbg_preflist(Key, N) ->
    IdxNode = lists:nth(N, get_dbg_preflist(Key)),
    {ok, req_id, Val} =
        riak_core_vnode_master:sync_command(IdxNode,
                                            {get, req_id, Key},
                                            dotted_db_vnode_master),
    [IdxNode, Val].