-module(dotted_db).
-include("dotted_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile({no_auto_import,[put/2,get/1]}).

-export([
         ping/0,
         vstate/0,
         new_client/0,
         new_client/1,
         get/1,
         get/2,
         put/2,
         put/3,
         put_at_node/3,
         put_at_node/4,
         delete/2,
         delete_at_node/3,
         sync/0,
         sync_at_node/1,
         test/0,
         test/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PUBLIC API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, dotted_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, dotted_db_vnode_master).


%% @doc Get the state from a random vnode.
vstate() ->
    DocIdx = riak_core_util:chash_key({<<"get_vnode_state">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, dotted_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, get_vnode_state, dotted_db_vnode_master).

%% @doc Returns a pair with this module name and the local node().
%% It can be used by client apps to connect to a DottedDB node and execute commands.
new_client() ->
    new_client(node()).
new_client(Node) ->
    case net_adm:ping(Node) of
        pang -> {error, {could_not_reach_node, Node}};
        pong -> {ok, {?MODULE, Node}}
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% UPDATES -> PUTs & DELETEs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Writes a Key-Value pair. The causal context is optional.
put(Key, Value) ->
    put(Key, Value, vv:new()).
put(Key, Value, Context) ->
    {ok, LocalNode} = new_client(),
    put_del([Key, Value, Context, ?WRITE_OP], LocalNode).

put_at_node(Key, Value, TargetNode) ->
    put_at_node(Key, Value, vv:new(), TargetNode).
put_at_node(Key, Value, Context, TargetNode) ->
    put_del([Key, Value, Context, ?WRITE_OP], TargetNode).

delete(Key, Context) ->
    {ok, LocalNode} = new_client(),
    put_del([Key, undefined, Context, ?DELETE_OP], LocalNode).

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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZATION
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Forces a anti-entropy synchronization with a vnode from the local node 
%% with another random vnode from a random node.
sync() ->
    sync_at_node({?MODULE, node()}).

%% @doc Forces a anti-entropy synchronization with a vnode from the received node 
%% with another random vnode from a random node.
sync_at_node({?MODULE, TargetNode}) ->
    Me = self(),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, Me],
    case node() of
        TargetNode ->
            dotted_db_sync_fsm_sup:start_sync_fsm(Request);
        _ ->
            proc_lib:spawn_link(TargetNode, dotted_db_sync_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).
    % {ok, Stats} = wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT),
    % stats:pp(Stats).





test(N) ->
    [test() || _ <- lists:seq(1,N)].

test() ->
    {not_found, _} = get("random_key"),
    K1 = dotted_db_utils:make_request_id(),
    ok = put(K1,"v1"),
    ok = put("key2","vb"),
    ok = put(K1,"v3"),
    ok = put("key3","vc"),
    ok = put(K1,"v2"),
    {ok, {Values, Ctx}} = get(K1),
    V123 = lists:sort(Values),
    ["v1","v2","v3"] = V123,
    ok = put(K1, "final", Ctx),
    {ok, {Final, Ctx2}} = get(K1),
    ["final"] = Final,
    ok = delete(K1,Ctx2),
    Del = get(K1),
    {not_found, _Ctx3} = Del,
    {ok, Stats1} = sync(),
    stats:pp(Stats1),
    {ok, Client} = new_client(),
    {ok, Stats2} = sync_at_node(Client),
    stats:pp(Stats2),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Unit Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

simple_test() ->
    ok = application:start(lager),
    ?assertNot(undefined == whereis(lager_sup)),
    ok = application:start(riak_core),
    ?assertNot(undefined == whereis(riak_core_sup)),
    ok = application:start(dotted_db),
    ?assertNot(undefined == whereis(dotted_db_sup)).

-endif.