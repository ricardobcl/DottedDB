-module(dotted_db_sync_manager).
-behaviour(gen_server).
-include_lib("dotted_db.hrl").

%% API
-export([start_link/0,
         enable/0,
         disable/0,
         set_sync_interval/1,
         get_sync_interval/0,
         set_kill_node_interval/1,
         get_kill_node_interval/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {sync_mode       :: on | off,
                sync_interval   :: non_neg_integer(),
                sync_timer      :: reference(),
                kill_mode       :: on | off,
                kill_interval   :: non_neg_integer(),
                kill_timer      :: reference()
               }).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enable() ->
    gen_server:call(?MODULE, enable, infinity).

disable() ->
    gen_server:call(?MODULE, disable, infinity).

set_sync_interval(Interval) ->
    gen_server:call(?MODULE, {set_sync_interval, Interval}, infinity).

get_sync_interval() ->
    gen_server:call(?MODULE, get_sync_interval, infinity).

set_kill_node_interval(Interval) ->
    gen_server:call(?MODULE, {set_kill_node_interval, Interval}, infinity).

get_kill_node_interval() ->
    gen_server:call(?MODULE, get_kill_node_interval, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {ok, state()}.
init([]) ->
    SyncTimer = schedule_sync(?DEFAULT_SYNC_INTERVAL),
    KillTimer = schedule_kill(?DEFAULT_NODE_KILL_RATE),
    % LocalVnodes = dotted_db_utils:vnodes_from_node(node()),
    State = #state{ sync_mode       = on,
                    sync_interval   = ?DEFAULT_SYNC_INTERVAL,
                    sync_timer      = SyncTimer,
                    kill_mode       = off,
                    kill_interval   = ?DEFAULT_NODE_KILL_RATE,
                    kill_timer      = KillTimer},
    {ok, State}.

handle_call(enable, _From, State) ->
    {reply, ok, State#state{sync_mode=on}};
handle_call(disable, _From, State) ->
    {reply, ok, State#state{sync_mode=off}};

handle_call(get_sync_interval, _From, State) ->
    {reply, {ok, State#state.sync_interval}, State};
handle_call({set_sync_interval, Interval}, _From, State) ->
    State#state.sync_timer =/= undefined andalso erlang:cancel_timer(State#state.sync_timer),
    lager:info("Setting new Sync interval: ~p", [Interval]),
    SyncTimer = schedule_sync(Interval),
    {reply, ok, State#state{sync_timer=SyncTimer, sync_interval=Interval}};

handle_call(get_kill_node_interval, _From, State=#state{kill_mode=off}) ->
    {reply, {ok, 0}, State};
handle_call(get_kill_node_interval, _From, State=#state{kill_mode=on}) ->
    {reply, {ok, State#state.kill_interval}, State};
handle_call({set_kill_node_interval, 0}, _From, State) ->
    lager:info("Turning off kill_node mode."),
    State#state.kill_timer =/= undefined andalso erlang:cancel_timer(State#state.kill_timer),
    {reply, ok, State#state{kill_interval=0, kill_mode=off, kill_timer=undefined}};
handle_call({set_kill_node_interval, Interval}, _From, State) ->
    State#state.kill_timer =/= undefined andalso erlang:cancel_timer(State#state.kill_timer),
    % properly seeding the process
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A,B,C}),
    %% randomize the first schedule, so that every machine is not killing nodes in-sync
    Interval2 = random:uniform(Interval),
    lager:info("Setting new kill_node: (random)> ~p (interval)> ~p", [Interval2, Interval]),
    KillTimer = schedule_kill(Interval2),
    {reply, ok, State#state{kill_interval=Interval, kill_mode=on, kill_timer=KillTimer}};

handle_call(Request, _From, State) ->
    lager:warning({unhandled_call, Request}),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:warning({unhandled_cast, Msg}),
    {noreply, State}.

handle_info(sync, State=#state{sync_mode=off, sync_interval=Interval}) ->
    SyncTimer = schedule_sync(Interval),
    {noreply, State#state{sync_timer=SyncTimer}};
handle_info(sync, State=#state{sync_mode=on, sync_interval=Interval}) ->
    SyncTimer = schedule_sync(Interval),
    {_, NextState} = sync(State#state{sync_timer=SyncTimer}),
    {noreply, NextState};

handle_info({_RefId, ok, sync}, State) ->
    {noreply, State};
handle_info({_RefId, timeout}, State) ->
    lager:warning("Sync request timeout!"),
    {noreply, State};
handle_info({_RefId, cancel, sync}, State) ->
    lager:warning("Sync node request canceled!"),
    {noreply, State};

handle_info(kill_node, State=#state{kill_mode=off}) ->
    {noreply, State};
handle_info(kill_node, State=#state{kill_mode=on, kill_interval=Interval}) ->
    KillTimer = schedule_kill(Interval),
    {_, NextState} = kill_node(State#state{kill_timer=KillTimer}),
    {noreply, NextState};

handle_info({_RefId, ok, restart, _NewID}, State) ->
    {noreply, State};
handle_info({_RefId, error, restart}, State) ->
    lager:warning("Kill node request error!"),
    {noreply, State};
handle_info({_RefId, timeout, restart}, State) ->
    lager:warning("Kill node request timeout!"),
    {noreply, State};
handle_info({_RefId, cancel, restart}, State) ->
    lager:warning("Kill node request canceled!"),
    {noreply, State};

handle_info({'DOWN', _RefId, _, _Pid, _Status}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    lager:warning({unhandled_info, Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec schedule_sync(non_neg_integer()) -> reference().
schedule_sync(Interval) ->
    %% Perform sync every X seconds
    erlang:send_after(Interval, ?MODULE, sync).

-spec sync(state()) -> {any(), state()}.
sync(State) ->
    % lager:debug("Start new sync"),
    ReqID = dotted_db_utils:make_request_id(),
    ThisNode = node(),
    case dotted_db_utils:vnodes_from_node(ThisNode) of
        [] ->
            lager:warning("No vnodes to do AAE sync on node ~p", [node()]),
            {ok, State};
        Vnodes ->
            VN = {_, ThisNode} = case Vnodes of
                [Vnode] -> Vnode;
                _ -> dotted_db_utils:random_from_list(Vnodes)
            end,
            case dotted_db_sync_fsm:start(ReqID, self(), VN) of
                {ok, FSMPid} ->
                    _Ref = monitor(process, FSMPid),
                    {ok, State};
                {error, Reason} ->
                    lager:warning("~p: error on sync_fsm:start. Reason:~p", [?MODULE, Reason]),
                    {Reason, State}
            end
    end.


-spec schedule_kill(non_neg_integer()) -> reference().
schedule_kill(0) ->
    undefined;
schedule_kill(Interval) ->
    %% Kill a Node every X seconds
    erlang:send_after(Interval, ?MODULE, kill_node).

-spec kill_node(state()) -> {any(), state()}.
kill_node(State) ->
    ReqID = dotted_db_utils:make_request_id(),
    ThisNode = node(),
    case dotted_db_utils:vnodes_from_node(ThisNode) of
        [] ->
            lager:warning("No vnodes to kill ~p", [node()]),
            {ok, State};
        Vnodes ->
            VN = {_, ThisNode} = case Vnodes of
                [Vnode] -> Vnode;
                _ -> dotted_db_utils:random_from_list(Vnodes)
            end,
            case dotted_db_restart_fsm:start_link(ReqID, self(), VN, []) of
                {ok, FSMPid} ->
                    _Ref = monitor(process, FSMPid),
                    {ok, State};
                {error, Reason} ->
                    lager:warning("~p: error on restart_fsm_sup:start. Reason:~p", [?MODULE, Reason]),
                    {Reason, State}
            end
    end.
