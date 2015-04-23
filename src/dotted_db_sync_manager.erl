-module(dotted_db_sync_manager).
-behaviour(gen_server).
-include_lib("dotted_db.hrl").

%% API
-export([start_link/0,
         enable/0,
         disable/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {mode           = on        :: on | off,
                tick           = 0         :: non_neg_integer()
                % nodes          = []        :: [index_node()]
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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {ok, state()}.
init([]) ->
    % Tick = app_helper:get_env(dotted_db, anti_entropy_tick, ?TICK),
    Tick = 4294967292,
    lager:info("Tick: ~p", [Tick]),
    schedule_tick(Tick),
    % LocalVnodes = dotted_db_utils:vnodes_from_node(node()),
    State = #state{ mode    = on,
                    tick    = Tick},
                    % nodes   = LocalVnodes},
    {ok, State}.

handle_call(enable, _From, State) ->
    {reply, ok, State#state{mode=on}};
handle_call(disable, _From, State) ->
    {reply, ok, State#state{mode=off}};
handle_call(Request, _From, State) ->
    lager:warning({unhandled_call, Request}),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:warning({unhandled_cast, Msg}),
    {noreply, State}.

handle_info(tick, State) ->
    State1 = maybe_tick(State),
    {noreply, State1};
handle_info({_RefId, ok, sync}, State) ->
    {noreply, State};
handle_info({_RefId, timeout}, State) ->
    lager:warning("Sync request timeout!"),
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


-spec maybe_tick(state()) -> state().
maybe_tick(State=#state{mode=off, tick=Tick}) ->
    schedule_tick(Tick),
    State;
maybe_tick(State=#state{mode=on, tick=Tick}) ->
    {_, NextState} = tick(State),
    schedule_tick(Tick),
    NextState.

-spec schedule_tick(non_neg_integer()) -> ok.
schedule_tick(Tick) ->
    %% Perform tick every X seconds
    erlang:send_after(Tick, ?MODULE, tick),
    ok.

-spec tick(state()) -> {any(), state()}.
tick(State) ->
    lager:debug("Start new tick for AAE Sync"),
    ReqID = dotted_db_utils:make_request_id(),
    case dotted_db_utils:vnodes_from_node(node()) of
        [] ->
            lager:warning("No vnodes to do AAE sync on node ~p", [node()]),
            {ok, State};
        Vnodes ->
            VN = case Vnodes of
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
