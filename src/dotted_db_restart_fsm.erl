%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(dotted_db_restart_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([restart/2, inform_peers/2, recovering_keys/2]).

-record(state, {
    %% Unique request ID.
    req_id      :: pos_integer(),
    %% Pid from the caller process.
    from        :: pid(),
    %% The vnode that will restart
    vnode       :: index_node(),
    %% Old id of the restarting vnode
    old_id      :: vnode_id(),
    %% New id of the restarting vnode
    new_id      :: vnode_id(),
    %% Peers of the restarting vnode
    peers       :: [index_node()],
    %% The timeout value for this request.
    timeout     :: non_neg_integer()
}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Vnode, Options) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Vnode, Options], []).

%%%===================================================================
%%% States
%%%===================================================================

%% Initialize state data.
init([ReqId, From, Vnode, Options]) ->
    lager:info("Restart Fsm: ~p.",[Vnode]),
    State = #state{ req_id      = ReqId,
                    from        = From,
                    vnode       = Vnode,
                    peers       = [],
                    timeout     = 2*proplists:get_value(?OPT_TIMEOUT, Options, ?DEFAULT_TIMEOUT)
    },
    {ok, restart, State, 0}.

%% @doc Execute the get reqs.
restart(timeout, State=#state{  req_id      = ReqId,
                                vnode       = Vnode}) ->
    dotted_db_vnode:restart([Vnode], ReqId),
    {next_state, inform_peers, State}.

%% @doc Wait for W-1 write acks. Timeout is 5 seconds by default (see dotted_db.hrl).
inform_peers(timeout, State=#state{ req_id      = ReqID,
                                    from        = From}) ->
    lager:warning("Restart Fsm: timeout in inform_peers state."),
    From ! {ReqID, timeout, restart},
    {stop, timeout, State};
inform_peers({cancel, ReqID, recovering}, State=#state{req_id = ReqID}) ->
    State#state.from ! {ReqID, cancel, restart},
    {stop, normal, State};
inform_peers({ok, ReqID, Args={_, _, OldVnodeID, NewVnodeID, _}, CurrentPeers}, State=#state{req_id = ReqID}) ->
    lager:info("Restart Fsm: inform peers: old ~p new ~p.",[OldVnodeID, NewVnodeID]),
    dotted_db_vnode:inform_peers_restart(CurrentPeers, Args),
    {next_state, recovering_keys,
        State#state{peers=CurrentPeers, old_id=OldVnodeID, new_id=NewVnodeID}}.

recovering_keys(timeout, State=#state{  req_id = ReqID,
                                        from   = From}) ->
    lager:warning("Restart Fsm: timeout in waiting for recovering nodes."),
    From ! {ReqID, error, restart},
    {stop, normal, State};

%% From one of the good nodes
recovering_keys({ok, stage1, ReqID, Args}, State=#state{req_id = ReqID}) ->
    lager:debug("Restart Fsm: stage1"),
    dotted_db_vnode:recover_keys([State#state.vnode], Args),
    {next_state, recovering_keys, State};
%% From the restarting node
recovering_keys({ok, stage2, ReqID, Peer}, State=#state{req_id = ReqID}) ->
    lager:debug("Restart Fsm: stage2"),
    dotted_db_vnode:inform_peers_restart2([Peer], {ReqID, State#state.new_id}),
    {next_state, recovering_keys, State};
%% From one of the good nodes
recovering_keys({ok, stage3, ReqID, Args}, State=#state{req_id = ReqID}) ->
    lager:debug("Restart Fsm: stage3"),
    dotted_db_vnode:recover_keys([State#state.vnode], Args),
    {next_state, recovering_keys, State};
%% From the restarting node
recovering_keys({ok, stage4, ReqID, Peer}, State=#state{
                                            req_id = ReqID,
                                            new_id = NewVnodeID,
                                            from   = From,
                                            peers  = RemainingPeers}) ->
    % lager:debug("Restart Fsm: stage4 rem: ~p", [RemainingPeers]),
    lager:debug("Restart Fsm: stage4"),
    case lists:delete(Peer, RemainingPeers) of
        [] ->
            From ! {ReqID, ok, restart, NewVnodeID},
            {stop, normal, State#state{peers = []}};
        RemainingPeers2 ->
            {next_state, recovering_keys, State#state{peers = RemainingPeers2}}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> 
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

