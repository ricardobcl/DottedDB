%% @doc FSM that synchronizes data in two replica nodes.
%% There are two modes:
%%      one_way: B -> A, sends data in B that's missing from A;
%%      two_way: A <-> B, bi-directional sync repair.
-module(dotted_db_sync_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start/3, start/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3, finalize/2]).

%% States
-export([   sync_start_A/2,
            sync_missing_B/2,
            sync_missing_A/2,
            sync_repair_AB/2,
            sync_ack/2]).


%% req_id: The request id so the caller can verify the response.
%%
%% sender: The pid of the sender so a reply can be made.
-record(state, {
    req_id          :: pos_integer(),
    mode            :: ?ONE_WAY | ?TWO_WAY,
    from            :: pid(),
    node_a          :: vnode(),
    id_a            :: vnode_id(),
    node_b          :: vnode(),
    id_b            :: vnode_id(),
    base_clock_b    :: vv(),
    miss_from_a     :: [{key(),dcc()}],
    timeout         :: non_neg_integer(),
    no_reply        :: boolean(),
    acks            :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start(ReqID, From, NodeA) ->
    start(ReqID, ?TWO_WAY, From, NodeA).
start(ReqID, Mode, From, NodeA) ->
    gen_fsm:start(?MODULE, [ReqID, Mode, From, NodeA], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, Mode, From, NodeA]) ->
    State = #state{
        req_id      = ReqID,
        mode        = Mode,
        from        = From,
        node_a      = NodeA,
        node_b      = undefined,
        timeout     = ?DEFAULT_TIMEOUT * 20, % sync is much slower than PUTs/GETs,
        no_reply    = ?DEFAULT_NO_REPLY,
        acks        = 0
    },
    {ok, sync_start_A, State, 0}.

%% @doc
sync_start_A(timeout, State=#state{ req_id  = ReqID,
                                    node_a  = NodeA}) ->
    dotted_db_vnode:sync_start([NodeA], ReqID),
    {next_state, sync_missing_B, State, State#state.timeout}.

%% @doc
sync_missing_B(timeout, State) ->
    State#state.from ! {State#state.req_id, timeout, { sync_missing_B, State#state.node_a } },
    {stop, normal, State};
sync_missing_B({cancel, ReqID, recovering}, State=#state{req_id = ReqID}) ->
    State#state.from ! {ReqID, cancel, sync},
    {stop, normal, State};
sync_missing_B({ok, ReqID, IdA={_,_}, NodeB, EntryBInClockA},
                            State=#state{ req_id = ReqID, mode = ?ONE_WAY}) ->
    dotted_db_vnode:sync_missing([NodeB], ReqID, IdA, EntryBInClockA),
    {next_state, sync_repair_AB, State#state{node_b=NodeB, id_a=IdA}, State#state.timeout};
sync_missing_B({ok, ReqID, IdA={_,_}, NodeB, EntryBInClockA},
                            State=#state{ req_id = ReqID, mode = ?TWO_WAY}) ->
    dotted_db_vnode:sync_missing([NodeB], ReqID, IdA, EntryBInClockA),
    {next_state, sync_missing_A, State#state{node_b=NodeB, id_a=IdA}, State#state.timeout}.

%% @doc
sync_missing_A(timeout, State) ->
    State#state.from ! {State#state.req_id, timeout, { sync_missing_A, State#state.node_a, State#state.node_b} },
    {stop, normal, State};
sync_missing_A({cancel, ReqID, recovering}, State=#state{req_id = ReqID}) ->
    State#state.from ! {ReqID, cancel, sync},
    {stop, normal, State};
sync_missing_A({ok, ReqID, IdB={_,_}, BaseClockB, EntryAInClockB, MissingFromA},
                    State=#state{   req_id  = ReqID,
                                    mode    = ?TWO_WAY,
                                    node_a  = NodeA,
                                    timeout = Timeout}) ->
    dotted_db_vnode:sync_missing([NodeA], ReqID, IdB, EntryAInClockB),
    {next_state, sync_repair_AB,
        State#state{id_b = IdB,
                    base_clock_b = BaseClockB,
                    miss_from_a  = MissingFromA},
        Timeout}.

%% @doc
sync_repair_AB(timeout, State) ->
    State#state.from ! {State#state.req_id, timeout, { sync_repair_AB,  State#state.node_a, State#state.node_b} },
    {stop, normal, State};
sync_repair_AB({cancel, ReqID, recovering}, State=#state{req_id = ReqID}) ->
    State#state.from ! {ReqID, cancel, sync},
    {stop, normal, State};
sync_repair_AB({ok, ReqID, IdB={_,_}, BaseClockB, _, MissingFromA},
        State=#state{   req_id      = ReqID,
                        mode        = ?ONE_WAY,
                        no_reply    = NoReply,
                        from        = From,
                        node_a      = NodeA}) ->
    dotted_db_vnode:sync_repair( [NodeA], {ReqID, IdB, BaseClockB, MissingFromA, NoReply}),
    case NoReply of
        true ->
            From ! {ReqID, ok, sync},
            {stop, normal, State#state{id_b = IdB}};
        false ->
            {next_state, sync_ack, State#state{id_b = IdB}, State#state.timeout}
    end;
sync_repair_AB({ok, ReqID, IdA={_,_}, BaseClockA, _, MissingFromB},
        State=#state{   req_id       = ReqID,
                        mode         = ?TWO_WAY,
                        no_reply     = NoReply,
                        from         = From,
                        node_a       = NodeA,
                        node_b       = NodeB,
                        id_a         = IdA,
                        id_b         = IdB,
                        base_clock_b = BaseClockB,
                        miss_from_a  = MissingFromA}) ->
    dotted_db_vnode:sync_repair( [NodeA], {ReqID, IdB, BaseClockB, MissingFromA, NoReply}),
    dotted_db_vnode:sync_repair( [NodeB], {ReqID, IdA, BaseClockA, MissingFromB, NoReply}),
    case NoReply of
        true ->
            From ! {ReqID, ok, sync},
            {stop, normal, State};
        false ->
            {next_state, sync_ack, State, State#state.timeout}
    end;
sync_repair_AB({ok, ReqID, IdA1={_,_}, _, _, _},
        State=#state{   req_id       = ReqID,
                        id_a         = IdA2}) when IdA1 =/= IdA2 ->
    State#state.from ! {ReqID, cancel, sync},
    {stop, normal, State}.

%% @doc
sync_ack(timeout, State) ->
    State#state.from ! {State#state.req_id, timeout, { sync_ack,  State#state.node_a, State#state.node_b} },
    {stop, normal, State};
sync_ack({cancel, ReqID, recovering}, State=#state{req_id = ReqID}) ->
    State#state.from ! {ReqID, cancel, sync},
    {stop, normal, State};
sync_ack({ok, ReqID},  State=#state{ req_id = ReqID, mode = ?ONE_WAY}) ->
    State#state.from ! {ReqID, ok, sync},
    {stop, normal, State#state{acks=1}};
sync_ack({ok, ReqID},  State=#state{ req_id = ReqID, mode = ?TWO_WAY, acks = 0}) ->
    {next_state, sync_ack, State#state{acks=1}, State#state.timeout};
sync_ack({ok, ReqID},  State=#state{ req_id = ReqID, mode = ?TWO_WAY, acks = 1}) ->
    State#state.from ! {ReqID, ok, sync},
    {stop, normal, State#state{acks=2}}.


handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

finalize(timeout, State) ->
    {stop, normal, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
