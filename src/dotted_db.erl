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
         put/1,
         put/2,
         delete/1,
         delete/2,
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
put(KeyValCtx) ->
    {ok, LocalNode} = new_client(),
    put(KeyValCtx, LocalNode).
put([Key, Value], TargetNode) ->
    put([Key, Value, vv:new()], TargetNode);
put([Key, Value, Context], TargetNode) ->
    put_del([Key, Value, Context, write], TargetNode).

delete(KeyCtx) ->
    {ok, LocalNode} = new_client(),
    delete(KeyCtx, LocalNode).
delete([Key], TargetNode) ->
    delete([Key, vv:new()], TargetNode);
delete([Key, Context], TargetNode) ->
    put_del([Key, undefined, Context, delete], TargetNode).

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
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    IdxNode = {_, ThisNode} = dotted_db_utils:random_index_node(),
    proc_lib:spawn_link(ThisNode, dotted_db_sync_fsm, start_link, [ReqID, Me, IdxNode]),
    {ok, Stats} = wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT),
    stats:pp(Stats).

sync_local() ->
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    ThisNode = node(),
    IdxNode = {_, ThisNode} = dotted_db_utils:random_local_index_node(),
    dotted_db_sync_fsm_sup:start_sync_fsm([ReqID, Me, IdxNode]),
    {ok, Stats} = wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT),
    stats:pp(Stats).


test() ->
    ok = put(["key1","va"]),
    ok = put(["key2","vb"]),
    ok = put(["key3","vc"]),
    ok = put(["key1","v2"]),
    ok = put(["key1","v3"]),
    {ok, {Val, VV}} = get("key1"),
    ?PRINT({Val, VV}),
    ok = put(["key1","final",VV]),
    {ok, {Val2, VV2}} = get("key1"),
    ?PRINT({Val2, VV2}).



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

%%%===================================================================
%%% Internal Functions
%%%===================================================================

wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, error, Error}   -> {error, Error};
        % get
        {ReqID, not_found}      -> {ok, not_found};
        {ReqID, ok, Reply}      -> {ok, decode_get_reply(Reply)};
        % put/delete
        {ReqID, ok}             -> ok;
        {ReqID, timeout}        -> {error, timeout}
    after Timeout ->
            {error, timeout}
    end.


decode_get_reply({BinValues, Context}) ->
    Values = [ dotted_db_utils:decode_kv(BVal) || BVal <- BinValues ],
    {Values, Context}.
