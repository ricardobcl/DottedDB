-module(dotted_db).
-include("dotted_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile({no_auto_import,[put/2,get/1]}).

-export([
         ping/0,
         vstate/0,
         vstates/0,
         new_client/0,
         new_client/1,
         get/1,
         get_at_node/2,
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


%% @doc Get the state from all vnodes.
vstates() ->
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, self(), vnode_state, ?DEFAULT_TIMEOUT],
    {ok, _} = dotted_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).


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
    do_get(Key, LocalNode).

get_at_node(Key, Client) ->
    do_get(Key, Client).


do_get(Key, {?MODULE, TargetNode}) ->
    BinKey = dotted_db_utils:encode_kv(Key),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, self(), BinKey],
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
    do_mutation([Key, Value, Context, ?WRITE_OP], LocalNode).

put_at_node(Key, Value, TargetNode) ->
    put_at_node(Key, Value, vv:new(), TargetNode).
put_at_node(Key, Value, Context, TargetNode) ->
    do_mutation([Key, Value, Context, ?WRITE_OP], TargetNode).

delete(Key, Context) ->
    {ok, LocalNode} = new_client(),
    do_mutation([Key, undefined, Context, ?DELETE_OP], LocalNode).

delete_at_node(Key, Context, TargetNode) ->
    do_mutation([Key, undefined, Context, ?DELETE_OP], TargetNode).



% @doc Writes normal PUTs and DELETEs
do_mutation([Key, Value, Context, Operation], {?MODULE, TargetNode}) ->
    BinKey = dotted_db_utils:encode_kv(Key),
    BinValue = dotted_db_utils:encode_kv(Value),
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, self(), Operation, BinKey, BinValue, Context],
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
    do_sync({?MODULE, node()}).

sync_at_node(Client) ->
    do_sync(Client).

%% @doc Forces a anti-entropy synchronization with a vnode from the received node 
%% with another random vnode from a random node.
do_sync({?MODULE, TargetNode}) ->
    ReqID = dotted_db_utils:make_request_id(),
    Request = [ReqID, self()],
    case node() of
        TargetNode ->
            dotted_db_sync_fsm_sup:start_sync_fsm(Request);
        _ ->
            proc_lib:spawn_link(TargetNode, dotted_db_sync_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).
    % {ok, Stats} = wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT),
    % dotted_db_utils:pp(Stats).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% OTHER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test(N) ->
    ok = dotted_db_stats:start(),
    Nodes = [ 'dotted_db1@127.0.0.1','dotted_db2@127.0.0.1',
              'dotted_db3@127.0.0.1','dotted_db4@127.0.0.1'],
    Res = [new_client(Node) || Node <- Nodes],
    Clients = [C || {ok, C} <- Res],
    F = fun() -> 
        Client = dotted_db_utils:random_from_list(Clients),
        ok = sync_at_node(Client)
        % io:format("\t Client:   \t ~p\n",[Client])
        % dotted_db_utils:pp(Stats1)
    end,
    [F() || _ <- lists:seq(1,N)],
    ok = dotted_db_stats:stop().

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
    ok = sync(),
    % dotted_db_utils:pp(Stats1),
    % {ok, Client} = new_client(),
    % {ok, Stats2} = sync_at_node_debug(Client),
    % dotted_db_utils:pp(Stats2),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for_reqid(ReqID, Timeout) ->
    receive
    %% Normal requests
        {ReqID, error, Error}               -> {error, Error};
        % get
        {ReqID, not_found, get, Context}    -> {not_found, Context};
        {ReqID, ok, get, Reply}             -> {ok, decode_get_reply(Reply)};
        % put/delete
        {ReqID, ok, update}                 -> ok;
        {ReqID, timeout}                    -> {error, timeout};
        % sync
        {ReqID, ok, sync}                   -> ok;
        % coverage
        {ReqID, ok, coverage, Results}      -> process_vnode_states(Results)
    after Timeout ->
        {error, timeout}
    end.


decode_get_reply({BinValues, Context}) ->
    Values = [ dotted_db_utils:decode_kv(BVal) || BVal <- BinValues ],
    {Values, Context}.

process_vnode_states(States) ->
    Results  = [ process_vnode_state(State) || State <- States ],
    Dots        = [ begin #{dots        := Res} = R , Res end || R<- Results ],
    ClockSize   = [ begin #{clock_size  := Res} = R , Res end || R<- Results ],
    Keys        = [ begin #{keys        := Res} = R , Res end || R<- Results ],
    KLSize      = [ begin #{kl_size     := Res} = R , Res end || R<- Results ],
    io:format("\n\n========= Vnodes ==========   \n"),
    io:format("\t Number of vnodes                  \t ~p\n",[length(States)]),
    io:format("\t Total     average miss_dots       \t ~p\n",[average(Dots)]),
    io:format("\t All       average miss_dots       \t ~p\n",[lists:sort(Dots)]),
    io:format("\t Average   clock size              \t ~p\n",[average(ClockSize)]),
    io:format("\t All       clock size              \t ~p\n",[lists:sort(ClockSize)]),
    io:format("\t Average   # keys in KL            \t ~p\n",[average(Keys)]),
    io:format("\t Per vnode # keys in KL            \t ~p\n",[lists:sort(Keys)]),
    io:format("\t Average   size keys in KL         \t ~p\n",[average(KLSize)]),
    io:format("\t Per vnode size keys in KL         \t ~p\n",[lists:sort(KLSize)]),
    ok.

process_vnode_state({Index, _Node, {ok,{state, _Id, Index, NodeClock, _Storage,
                    _Replicated, KeyLog, _Dets, _Updates_mem, _Stats}}}) ->
    % ?PRINT(NodeClock),
    MissingDots = [ miss_dots(Entry) || {_,Entry} <- NodeClock ],
    {Keys, Size} = KeyLog,
    #{   
          dots          => average(MissingDots)
        , clock_size    => byte_size(term_to_binary(NodeClock))
        , keys          => Keys %length(Keys)
        , kl_size       => Size %byte_size(term_to_binary(KeyLog))
    }.


miss_dots({N,B}) ->
    case values_aux(N,B,[]) of
        [] -> 0;
        L  -> lists:max(L) - N - length(L)
    end.
values_aux(_,0,L) -> L;
values_aux(N,B,L) ->
    M = N + 1,
    case B rem 2 of
        0 -> values_aux(M, B bsr 1, L);
        1 -> values_aux(M, B bsr 1, [ M | L ])
    end.

average(X) ->
    average(X, 0, 0).
average([H|T], Length, Sum) ->
    average(T, Length + 1, Sum + H);
average([], Length, Sum) ->
    Sum / Length.

        % % node id used for in logical clocks
        % id          :: id(),
        % % index on the consistent hashing ring
        % index       :: index(),
        % % node logical clock
        % clock       :: bvv(),
        % % key->value store, where the value is a DCC (values + logical clock)
        % storage     :: dotted_db_storage:storage(),
        % % what peer nodes have from my coordinated writes (not real-time)
        % replicated  :: vv(),
        % % log for keys that this node coordinated a write (eventually older keys are safely pruned)
        % keylog      :: keylog(),
        % % number of updates (put or deletes) since saving node state to storage
        % updates_mem :: integer(),
        % % DETS table that stores in disk the vnode state
        % dets        :: dets(),
        % % a flag to collect or not stats
        % stats       :: boolean()