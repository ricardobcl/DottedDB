%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(dotted_db_get_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute/2, waiting/2]).

-record(state, {
    req_id      :: pos_integer(),
    from        :: pid(),
    key         :: string(),
    replicas    :: riak_core_apl:preflist2(),
    acks        :: non_neg_integer(),
    good_acks   :: non_neg_integer(),
    reply       :: dcc(),
    timeout     :: non_neg_integer(),
    debug       :: boolean(),
    stats       :: #{}
}).

%%%===================================================================
%%% API
%%%===================================================================

% get(ReqId, From, Key) ->
%     dotted_db_get_fsm_sup:start_get_fsm([ReqID, From, Key]),
%     {ok, ReqID}.

start_link(ReqID, From, Key, Debug) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Key, Debug], []).

%%%===================================================================
%%% States
%%%===================================================================

%% Initialize state data.
init([ReqId, From, Key, Debug]) ->
    SD = #state{req_id      = ReqId,
                from        = From,
                key         = Key,
                replicas    = dotted_db_utils:replica_nodes(Key),
                acks        = 0,
                good_acks   = 0,
                reply       = dcc:new(),
                timeout     = ?DEFAULT_TIMEOUT,
                debug       = Debug,
                stats       = #{}
    },
    {ok, execute, SD, 0}.

% %% @doc Calculate the Replica Nodes.
% prepare(timeout, State=#state{key=Key}) ->
%     % add an entry in the read requests to track responses from remote nodes
%     Replies = dict:store(RequestId, {MinResponses, dcc:new()}, State#state.reads),
%     {next_state, execute, State#state{replies = Replies, replicas=dotted_db_utils:replica_nodes(Key)}, 0}.

%% @doc Execute the get reqs.
execute(timeout, State=#state{  req_id      = ReqId,
                                key         = Key,
                                debug       = Debug,
                                replicas    = ReplicaNodes}) ->
    Stats = case Debug of 
        true -> #{}; %dotted_db_stats:start();
        false -> #{}
    end,
    % request this key from nodes that store it (ReplicaNodes)
    dotted_db_vnode:read(ReplicaNodes, ReqId, Key, Debug),
    {next_state, waiting, State#state{stats=Stats}}.

%% @doc Wait for W-1 write acks. Timeout is 5 seconds by default (see dotted_db.hrl).
waiting(timeout, State=#state{  req_id      = ReqID,
                                from        = From}) ->
    lager:warning("GET_FSM timeout in waiting state."),
    From ! {ReqID, timeout},
    {stop, timeout, State};

waiting({ok, ReqID, Response, NewStats}, State=#state{    req_id      = ReqID,
                                                from        = From,
                                                reply       = Reply,
                                                acks        = Acks,
                                                stats       = Stats,
                                                debug       = Debug,
                                                good_acks   = GoodAcks}) ->
    Stats2 = case Debug of 
        true -> maps:merge(NewStats, Stats);
        false -> #{}
    end,
    % synchronize with the current object, or don't if the response is not_found
    % not_found still counts as a valid response
    {NewGoodAcks, NewAcks, MaybeError, NewReply} =
        case Response of
            {error, Error}  -> {GoodAcks  , Acks+1, Error    , Reply};
            _               -> {GoodAcks+1, Acks+1, no_error, dcc:sync(Response,Reply)}
        end,
    NewState = State#state{acks = NewAcks, good_acks = NewGoodAcks, reply = NewReply, stats=Stats2},
    % test if we have enough responses to respond to the client
    case NewGoodAcks >= ?R of
        true -> % we already have enough responses to acknowledge back to the client
            {NewState2, Stats3} = case Debug of 
                true -> 
                    % Stats0 = dotted_db_stats:stop(Stats2),
                    {NewState#state{stats=Stats2}, Stats2};
                false -> 
                    {NewState, #{}}
            end,
            case {dcc:values(NewReply) =:= [], Debug} of
                {true, false} -> % no response found; return the context for possibly future writes
                    From ! {ReqID, not_found, get, dcc:context(NewReply)};
                {false, false} -> % there is at least on value for this key
                    From ! {ReqID, ok, get, {dcc:values(NewReply), dcc:context(NewReply)}};
                {true, true} -> % no response found; return the context for possibly future writes
                    From ! {ReqID, not_found, get, dcc:context(NewReply), Stats3};
                {false, true} -> % there is at least on value for this key
                    From ! {ReqID, ok, get, {dcc:values(NewReply), dcc:context(NewReply)}, Stats3}
            end,
            {stop, normal, NewState2};
        false -> % we still need more (good) responses
            case NewAcks >= ?N of
                true  -> % not enough good nodes responded, return error
                    From ! {ReqID, error, MaybeError},
                    {stop, normal, NewState};
                false -> % we still miss some responses from replica nodes 
                    {next_state, waiting, NewState}
            end
    end.


% finalize(timeout, State=#state{ req_id      = ReqID, 
%                                 reply       = Reply, 
%                                 from        = From}) ->
%     ?PRINT("finalize :)"),
%     case Reply =:= dcc:new() of
%         true -> % no response found
%             ?PRINT("fin: not found"),
%             From ! {ReqID, not_found};
%         false -> % there an answer
%             ?PRINT("fin: good"),
%             From ! {ReqID, ok, {dcc:values(Reply), dcc:context(Reply)}}
%     end,
%     % MObj = merge(Replies),
%     % case needs_repair(MObj, Replies) of
%     %     true ->
%     %         repair(Key, MObj, Replies),
%     %         {stop, normal, SD};
%     %     false ->
%     %         {stop, normal, SD}
%     % end.
%     {stop, normal, State};
% finalize({ok, ReqID, _Response}, State=#state{req_id = ReqID}) ->
%     ?PRINT("finalize: discard"),
%     {stop, normal, State}.

% %% @doc Wait for R replies and then respond to "From", the original client
% %% that called `rts:get/2'.
% waiting({ok, ReqID, IdxNode, Obj},
%         SD0=#state{from=From, num_r=NumR0, replies=Replies0,
%                    r=R, timeout=Timeout}) ->
%     NumR = NumR0 + 1,
%     Replies = [{IdxNode, Obj}|Replies0],
%     SD = SD0#state{num_r=NumR,replies=Replies},

%     if
%         NumR =:= R ->
%             % Reply = rts_obj:val(merge(Replies)),
%             Reply = "nice",
%             From ! {ReqID, ok, Reply},

%             if NumR =:= ?N -> {next_state, finalize, SD, 0};
%                true -> {next_state, wait_for_n, SD, Timeout}
%             end;
%         true -> {next_state, waiting, SD}
%     end.

% wait_for_n({ok, _ReqID, IdxNode, Obj},
%              SD0=#state{num_r=?N - 1, replies=Replies0, key=_Key}) ->
%     Replies = [{IdxNode, Obj}|Replies0],
%     {next_state, finalize, SD0#state{num_r=?N, replies=Replies}, 0};

% wait_for_n({ok, _ReqID, IdxNode, Obj},
%              SD0=#state{num_r=NumR0, replies=Replies0,
%                         key=_Key, timeout=Timeout}) ->
%     NumR = NumR0 + 1,
%     Replies = [{IdxNode, Obj}|Replies0],
%     {next_state, wait_for_n, SD0#state{num_r=NumR, replies=Replies}, Timeout};

% %% TODO partial repair?
% wait_for_n(timeout, SD) ->
%     {stop, timeout, SD}.


handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.


%%%===================================================================
%%% Internal Functions
%%%===================================================================


% %% @pure
% %%
% %% @doc Given a list of `Replies' return the merged value.
% -spec merge([vnode_reply()]) -> rts_obj() | not_found.
% merge(Replies) ->
%     Objs = [Obj || {_,Obj} <- Replies],
%     rts_obj:merge(Objs).



% %% @pure
% %%
% %% @doc Given the merged object `MObj' and a list of `Replies'
% %% determine if repair is needed.
% -spec needs_repair(any(), [vnode_reply()]) -> boolean().
% needs_repair(MObj, Replies) ->
%     Objs = [Obj || {_,Obj} <- Replies],
%     lists:any(different(MObj), Objs).

% %% @pure
% different(A) -> fun(B) -> not rts_obj:equal(A,B) end.

% %% @impure
% %%
% %% @doc Repair any vnodes that do not have the correct object.
% -spec repair(string(), rts_obj(), [vnode_reply()]) -> io.
% repair(_, _, []) -> io;

% repair(Key, MObj, [{IdxNode,Obj}|T]) ->
%     case rts_obj:equal(MObj, Obj) of
%         true -> repair(Key, MObj, T);
%         false ->
%             rts_stat_vnode:repair(IdxNode, Key, MObj),
%             repair(Key, MObj, T)
%     end.

% %% pure
% %%
% %% @doc Given a list return the set of unique values.
% -spec unique([A::any()]) -> [A::any()].
% unique(L) ->
%     sets:to_list(sets:from_list(L)).
