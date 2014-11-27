%% @doc The coordinator for stat write operations.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(dotted_db_put_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3, finalize/2]).

%% States
-export([prepare/2, write/2, waiting_coordinator/2, waiting_replicas/2]).

-record(state, {
    req_id          :: pos_integer(),
    from            :: pid(),
    coordinator     :: node(),
    operation       :: operation(), 
    key             :: key(),
    value           :: term() | undefined,
    context         :: vv(),
    replicas        :: riak_core_apl:preflist2(),
    acks            :: non_neg_integer(),
    completed       :: boolean(),
    timeout         :: non_neg_integer(),
    stats           :: stats_reqs()
}).

-type operation() :: ?WRITE_OP | ?DELETE_OP.

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Operation, Key, Value, Context) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Operation, Key, Value, Context], []).


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Operation, Key, Value, Context]) ->
    SD = #state{
        req_id      = ReqID,
        coordinator = undefined,
        from        = From,
        operation   = Operation,
        key         = Key,
        value       = Value,
        context     = Context,
        replicas    = dotted_db_utils:replica_nodes(Key),
        acks        = 0,
        completed   = false,
        timeout     = ?DEFAULT_TIMEOUT
        % stats       = stats:new_req()
    },
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, State=#state{  req_id      = ReqID,
                                from        = From,
                                operation   = Op,
                                key         = Key,
                                value       = Value,
                                context     = Context,
                                replicas    = Replicas}) ->
    Coordinator = [IndexNode || {_Index, Node} = IndexNode <- Replicas, Node == node()],
    case Coordinator of
        [] -> % this is not replica node for this key -> forward the request
            {ListPos, _} = random:uniform_s(length(Replicas), os:timestamp()),
            {_Idx, CoordNode} = lists:nth(ListPos, Replicas),
            proc_lib:spawn_link(CoordNode, dotted_db_put_fsm, start_link,
                                    [ReqID, From, Op, Key, Value, Context]),
            {stop, normal, State};
            % we could wait for an ack, to avoid bad coordinators and request being lost
            % see riak_kv_put_fsm.erl:197
        _ -> % this is a replica node, thus can coordinate write/delete
            {next_state, write, State#state{coordinator=Coordinator}, 0}
    end.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
write(timeout, State=#state{req_id      = ReqID,
                            coordinator = Coordinator,
                            operation   = Operation,
                            key         = Key,
                            value       = Value,
                            context     = Context}) ->
    % % add an entry in the write requests for this key, to track acks from remote nodes
    % Writes = dict:store(Key, sets:new(), State#state.writes),
    dotted_db_vnode:write(Coordinator, ReqID, Operation, Key, Value, Context),
    {next_state, waiting_coordinator, State}.

%% @doc Coordinator writes the value.
waiting_coordinator({ok, ReqID, DCC}, State=#state{ req_id      = ReqID,
                                                    coordinator = Coordinator,
                                                    from        = From,
                                                    key         = Key,
                                                    acks        = Acks,
                                                    replicas    = Replicas,
                                                    timeout     = Timeout}) ->
    Acks2 = Acks + 1,
    Completed = Acks2 >= ?W,
    % if we have enough write acknowledgments, reply back to the client
    case Completed of
        true  -> From ! {ReqID, ok};
        false -> ok
    end,
    % replicate the new object to other replica nodes, except the coordinator
    dotted_db_vnode:replicate(Replicas -- Coordinator, ReqID, Key, DCC),
    {next_state, waiting_replicas, State#state{acks=Acks2, completed=Completed}, Timeout}.


%% @doc Wait for W-1 write acks. Timeout is 5 seconds by default (see dotted_db.hrl).
waiting_replicas(timeout, State=#state{     req_id      = ReqID,
                                            from        = From,
                                            completed   = Completed}) ->
    lager:warning("replicated timeout!!"),
    case Completed of
        true  ->
            {stop, normal, State};
        false ->
            From ! {ReqID, timeout},
            {stop, timeout, State}
    end;
waiting_replicas({ok, ReqID}, State=#state{ req_id      = ReqID,
                                            from        = From,
                                            acks        = Acks,
                                            completed   = Completed}) ->
    Acks2 = Acks + 1,
    Completed2 = case Acks2 >= ?W andalso not Completed of
        true  ->
            From ! {ReqID, ok},
            true;
        false ->
            false
    end,
    case Acks2 >= ?N of
        true  ->
            {stop, normal, State#state{acks=Acks2, completed=Completed2}};
        false ->
            {next_state, waiting_replicas, State#state{acks=Acks2, completed=Completed2}}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> 
    {ok, StateName, State}.

finalize(timeout, State=#state{key=_Key}) ->
    % MObj = merge(Replies),
    % case needs_repair(MObj, Replies) of
    %     true ->
    %         repair(Key, MObj, Replies),
    %         {stop, normal, SD};
    %     false ->
    %         {stop, normal, SD}
    % end.
    {stop, normal, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
