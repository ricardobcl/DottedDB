%% @doc
-module(dotted_db_sync_fsm).
-behavior(gen_fsm).
-include("dotted_db.hrl").

%% API
-export([start/3]).

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
    vnode           :: vnode(),
    peer            :: vnode(),
    timeout         :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start(ReqID, From, Vnode) ->
    gen_fsm:start(?MODULE, [ReqID, From, Vnode], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Vnode]) ->
    State = #state{
        req_id      = ReqID,
        from        = From,
        vnode       = Vnode,
        peer        = undefined,
        timeout     = ?DEFAULT_TIMEOUT * 20 % sync is much slower than PUTs/GETs,
    },
    {ok, sync_start, State, 0}.

%% @doc
sync_start(timeout, State=#state{   req_id  = ReqID,
                                    vnode   = Vnode}) ->
    % get the ring index for thing vnode
    {Index, _ } = Vnode,
    % get this node's peers, i.e., all nodes that replicates any subset of local keys
    Peers = dotted_db_utils:peers(Index),
    % choose a random node from that list
    Peer = dotted_db_utils:random_from_list(Peers),
    dotted_db_vnode:sync_start([Vnode], Peer, ReqID),
    {next_state, sync_request, State#state{peer=Peer}, State#state.timeout}.

%% @doc
sync_request({ok, ReqID, RemoteNodeID, RemoteEntry}, State=#state{peer = Peer}) ->
    dotted_db_vnode:sync_request(Peer, ReqID, RemoteNodeID, RemoteEntry),
    {next_state, sync_response, State, State#state.timeout}.

%% @doc
sync_response({ok, ReqID, _, _, []}, State) ->
    State#state.from ! {ReqID, ok, sync},
    {stop, normal, State};
sync_response({ok, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects}, State=#state{vnode = Vnode}) ->
    dotted_db_vnode:sync_response( [Vnode], ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects),
    {next_state, sync_ack, State, State#state.timeout}.

%% @doc
sync_ack({ok, ReqID}, State) ->
    State#state.from ! {ReqID, ok, sync},
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
    {stop, normal, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
