%% @doc 
-module(dotted_db_sync_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3, finalize/2]).

%% States
-export([sync_start/2, sync_request/2, sync_response/2, sync_ack/2]).


%% req_id: The request id so the caller can verify the response.
%%
%% sender: The pid of the sender so a reply can be made.
-record(state, {
    req_id          :: pos_integer(),
    from            :: pid(),
    index_node      :: index_node(),
    peer            :: index_node(),
    timeout         :: non_neg_integer(),
    debug           :: boolean(),
    stats           :: #{}
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Debug) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Debug], []).


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Debug]) ->
    ThisNode = node(),
    IdxNode = {_, ThisNode} = dotted_db_utils:random_index_from_node(ThisNode),
    SD = #state{
        req_id      = ReqID,
        from        = From,
        index_node  = IdxNode,
        peer        = undefined,
        timeout     = ?DEFAULT_TIMEOUT * 20, % sync is much slower than PUTs/GETs,
        debug       = Debug,
        stats       = #{}
    },
    {ok, sync_start, SD, 0}.

%% @doc 
sync_start(timeout, State=#state{   req_id      = ReqID,
                                    debug       = Debug,
                                    index_node  = IdxNode}) ->
    Stats = case Debug of 
        true -> #{}; %dotted_db_stats:start();
        false -> #{}
    end,
    dotted_db_vnode:sync_start([IdxNode], ReqID, Debug),
    {next_state, sync_request, State#state{stats=Stats}, State#state.timeout}.

%% @doc 
sync_request({ok, ReqID, Peer, RemoteNodeID, RemoteEntry, Stats}, State) ->
    Stats2 = maps:merge(State#state.stats, Stats),
    dotted_db_vnode:sync_request(Peer, ReqID, RemoteNodeID, RemoteEntry, State#state.debug),
    {next_state, sync_response, State#state{peer=Peer,stats=Stats2}, State#state.timeout}.

%% @doc 
sync_response({ok, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects, Stats}, State) ->
    Stats2 = maps:merge(State#state.stats, Stats),
    dotted_db_vnode:sync_response( [State#state.index_node],
        ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects, State#state.debug),
    {next_state, sync_ack, State#state{stats=Stats2}, State#state.timeout}.

%% @doc 
sync_ack({ok, ReqID, Stats}, State) ->
    case State#state.debug of
        true ->
            Stats3 = maps:merge(State#state.stats, Stats),
            % Stats3 = dotted_db_stats:stop(Stats2),
            State#state.from ! {ReqID, ok, sync, Stats3};
        false ->
            State#state.from ! {ReqID, ok, sync}
    end,
    {stop, normal, State}.




handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> 
    {ok, StateName, State}.

finalize(timeout, State) ->
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
