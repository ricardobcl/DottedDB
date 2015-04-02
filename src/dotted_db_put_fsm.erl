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
    %% The node coordination this request (must be a replica node for this key).
    coordinator     :: node(),
    %% Operation can be a write or a delete.
    operation       :: operation(),
    %$ The key being written.
    key             :: bkey(),
    %$ The new value being written.
    value           :: term() | undefined,
    %$ The causal context of this request.
    context         :: vv:vv(),
    %% Number of replica nodes contacted to write/delete.
    replication     :: non_neg_integer(),
    %% Replica Nodes for this key.
    replicas        :: riak_core_apl:preflist2(),
    %% Minimal number of acks from replica nodes.
    min_acks        :: non_neg_integer(),
    %% Current number of acks received by successful remote writes.
    acks            :: non_neg_integer(),
    %% Indicates if this request is completed.
    completed       :: boolean(),
    %% Timeout for the request.
    timeout         :: non_neg_integer(),
    %% The options proplist.
    options         :: list() % proplist()
}).

-type operation() :: ?WRITE_OP | ?DELETE_OP.

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, BKey, Value, Context, Options) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, BKey, Value, Context, Options], []).


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, BKey, Value, Context, Options]) ->
    MinAcks = proplists:get_value(?OPT_PUT_MIN_ACKS, Options),
    Replication = proplists:get_value(?OPT_PUT_REPLICAS, Options),
    %% Sanity check
    true = ?REPLICATION_FACTOR >= MinAcks,
    true = ?REPLICATION_FACTOR >= Replication,
    true = Replication >= MinAcks,
    Operation = case proplists:is_defined(?WRITE_OP, Options) of
        true -> ?WRITE_OP;
        false -> ?DELETE_OP
    end,
    SD = #state{
        req_id      = ReqID,
        coordinator = undefined,
        from        = From,
        operation   = Operation,
        key         = BKey,
        value       = Value,
        context     = Context,
        replication = Replication,
        replicas    = dotted_db_utils:replica_nodes(BKey),
        min_acks    = MinAcks,
        acks        = 0,
        completed   = false,
        timeout     = proplists:get_value(?OPT_TIMEOUT, Options, ?DEFAULT_TIMEOUT),
        options     = Options
    },
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, State=#state{  req_id      = ReqID,
                                from        = From,
                                key         = BKey,
                                value       = Value,
                                context     = Context,
                                replicas    = Replicas,
                                options     = Options}) ->
    Coordinator = [IndexNode || {_Index, Node} = IndexNode <- Replicas, Node == node()],
    case Coordinator of
        [] -> % this is not replica node for this key -> forward the request
            {_Idx, CoordNode} = dotted_db_utils:random_from_list(Replicas),
            proc_lib:spawn_link(CoordNode, dotted_db_put_fsm, start_link,
                                    [ReqID, From, BKey, Value, Context, Options]),
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
                                                    key         = BKey,
                                                    min_acks    = MinAcks,
                                                    acks        = Acks,
                                                    replication = Replication,
                                                    replicas    = Replicas,
                                                    timeout     = Timeout}) ->
    % if we have enough write acknowledgments, reply back to the client
    Completed = case Acks + 1 >= MinAcks of
                    true  ->
                        From ! {ReqID, ok},
                        true;
                    false ->
                        false
                end,
    case Acks + 1 >= Replication of
        true  -> %% If true, we don't want to replicate to more replica nodes.
            % ?PRINT("PUT_FSM: (1):"),
            % ?PRINT(Acks+1),
            {stop, normal, State};
        false ->  %% Else, replicate to the remaining number of replica nodes, according to `Replication`
            Replicas2 = dotted_db_utils:random_sublist(Replicas -- Coordinator, Replication - 1),
            % ?PRINT("PUT_FSM: (3):"),
            % ?PRINT(length(Replicas2) + 1),
            % ?PRINT("PUT FSM: I'm replicating to ~p replica nodes in total.", [length(Replicas2) + 1]),
            dotted_db_vnode:replicate(Replicas2, ReqID, BKey, DCC),
            {next_state, waiting_replicas, State#state{acks=Acks+1, completed=Completed}, Timeout}
    end.

%% @doc Wait for W-1 write acks. Timeout is 20 seconds by default (see dotted_db.hrl).
waiting_replicas(timeout, State=#state{     completed   = true }) ->
    lager:warning("Replicated timeout!!"),
    {stop, normal, State};
waiting_replicas(timeout, State=#state{     req_id      = ReqID,
                                            from        = From,
                                            completed   = false}) ->
    lager:warning("Replicated timeout!!"),
    From ! {ReqID, timeout},
    {stop, timeout, State};
waiting_replicas({ok, ReqID}, State=#state{ req_id      = ReqID,
                                            from        = From,
                                            acks        = Acks,
                                            min_acks    = MinAcks,
                                            replication = Replication,
                                            completed   = Completed}) ->
    NewState = case Acks + 1 >= MinAcks andalso not Completed of
        true  ->
            From ! {ReqID, ok, update},
            State#state{acks=Acks+1, completed=true};
        false ->
            State#state{acks=Acks+1}
    end,
    case Acks + 1 >= Replication of
        true  ->
            % ?PRINT("PUT_FSM: (2):"),
            % ?PRINT(Acks+1),
            {stop, normal, NewState};
        false ->
            {next_state, waiting_replicas, NewState}
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
