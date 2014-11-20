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
    stats           :: stats_sync()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, IdxNode) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, IdxNode], []).


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, IdxNode]) ->
    SD = #state{
        req_id      = ReqID,
        from        = From,
        index_node  = IdxNode,
        peer        = undefined,
        timeout     = ?DEFAULT_TIMEOUT * 20, % sync is much slower than PUTs/GETs
        stats       = stats:new_sync()
    },
    {ok, sync_start, SD, 0}.

%% @doc 
sync_start(timeout, State=#state{   req_id      = ReqID,
                                    index_node  = IdxNode,
                                    stats       = Stats}) ->
    Stats2 = stats:start_sync(Stats),
    dotted_db_vnode:sync_start([IdxNode], ReqID),
    {next_state, sync_request, State#state{stats=Stats2}, State#state.timeout}.

%% @doc 
sync_request({ok, ReqID, Peer, RemoteNodeID, RemoteEntry, _Stats={NodeA, NodeB}}, State) ->
    Stats = stats:nodes(State#state.stats, NodeA, NodeB),
    dotted_db_vnode:sync_request(Peer, ReqID, RemoteNodeID, RemoteEntry),
    {next_state, sync_response, State#state{peer=Peer,stats=Stats}, State#state.timeout}.

%% @doc 
sync_response({ok, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects, Stats}, State) ->
    Stats2 = stats:sync_req(State#state.stats, Stats),
    dotted_db_vnode:sync_response(
            [State#state.index_node], ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects),
    {next_state, sync_ack, State#state{stats=Stats2}, State#state.timeout}.

%% @doc 
sync_ack({ok, ReqID}, State) ->
    Stats = stats:end_sync(State#state.stats),
    State#state.from ! {ReqID, {ok, Stats}},
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
