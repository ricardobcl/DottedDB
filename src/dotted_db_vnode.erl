-module(dotted_db_vnode).
-behaviour(riak_core_vnode).
-include_lib("dotted_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-export([
         read/3,
         write/6,
         replicate/4,
         sync_start/2,
         sync_request/4,
         sync_response/5
        ]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {
        % node id used for in logical clocks
        id          :: id(),
        % index on the consistent hashing ring
        index       :: index(),
        % node logical clock
        clock       :: bvv(),
        % key->value store, where the value is a DCC (values + logical clock)
        objects     :: dotted_db_storage:storage(),
        % what peer nodes have from my coordinated writes (not real-time)
        replicated  :: vv(),
        % log for keys that this node coordinated a write (eventually older keys are safely pruned)
        keylog      :: keylog()
    }).


-define(MASTER, dotted_db_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


read(ReplicaNodes, ReqID, Key) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {read, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).


write(Coordinator, ReqID, Op, Key, Value, Context) ->
    riak_core_vnode_master:command(Coordinator,
                                   {write, ReqID, Op, Key, Value, Context},
                                   {fsm, undefined, self()},
                                   ?MASTER).


replicate(ReplicaNodes, ReqID, Key, DCC) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {replicate, ReqID, Key, DCC},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_start(Node, ReqID) ->
    riak_core_vnode_master:command(Node,
                                   {sync_start, ReqID},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_request(Peer, ReqID, RemoteNodeID, RemoteEntry) ->
    riak_core_vnode_master:command(Peer,
                                   {sync_request, ReqID, RemoteNodeID, RemoteEntry},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_response(Node, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects) ->
    riak_core_vnode_master:command(Node,
                                   {sync_response, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects},
                                   {fsm, undefined, self()},
                                   ?MASTER).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([Index]) ->
    DBName = "data/objects/" ++ integer_to_list(Index),
    Backend = case app_helper:get_env(dotted_db, storage_backend) of
        leveldb     -> {backend, leveldb};
        ets         -> {backend, ets};
        _SB         -> {backend, ets}
    end,
    {ok, Storage} = dotted_db_storage:open(DBName, [Backend]),
    % get this node's peers, i.e., all nodes that replicates any subset of local keys
    PeerIDs = [ ID || {ID, _Node} <- dotted_db_utils:peers(Index)],
    % for replication factor N = 3, the numbers of peers should be 4 (2 vnodes before and 2 after)
    (?N-1)*2 = length(PeerIDs),
    Replicated = lists:foldl(fun (ID,VV) -> vv:add(VV,{ID,0}) end , vv:new(), PeerIDs),
    (?N-1)*2 = length(Replicated),
    S = #state{
        id          = Index, % or {Index, node()},
        index       = Index,
        clock       = bvv:new(),
        objects     = Storage,
        replicated  = Replicated,
        keylog      = {0,[]}
    },
    {ok, S}.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({read, ReqID, Key}, _Sender, State) ->
    Response =
        case dotted_db_storage:get(State#state.objects, Key) of
            {error, not_found} -> 
                % there is no key K in this node
                % create an empty "object" and fill its causality with the node clock
                % this is needed to ensure that deletes "win" over old writes at the coordinator
                dcc:fill(dcc:new(), State#state.clock);
            {error, Error} -> 
                % some unexpected error
                lager:error("Error reading a key from storage (command read): ~p", [Error]),
                % return the error
                {error, Error};
            DCC ->
                % get and fill the causal history of the local object
                dcc:fill(DCC, State#state.clock)
        end,
    {reply, {ok, ReqID, Response}, State};




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({write, ReqID, Operation, Key, Value, Context}, _Sender, State) ->
    % get and fill the causal history of the local key
    DiskDCC = guaranteed_get(Key, State),
    % discard obsolete values w.r.t the causal context
    DiscardDCC = dcc:discard(DiskDCC, Context),
    % generate a new dot for this write/delete and add it to the node clock
    {Dot, NodeClock} = bvv:event(State#state.clock, State#state.id),
    % test if this is a delete; if not, add dot-value to the DCC container
    NewDCC =
        case Operation of
            ?DELETE_OP  -> % DELETE
                DiscardDCC;
            ?WRITE_OP   -> % PUT
                dcc:add(DiscardDCC, {State#state.id, Dot}, Value)
        end,
    % removed unnecessary causality from the DCC, based on the current node clock
    StrippedDCC = dcc:strip(NewDCC, NodeClock),
    % check if the resulting object/DCC is empty (i.e. it was deleted and has no causal history)
    case StrippedDCC  =:= dcc:new() of
        true -> % we can safely remove this key from disk (distributed deletes done right :-))
            ok = dotted_db_storage:delete(State#state.objects, Key);
        false -> % we still have relevant information (PUT or DELETE).
        % this can still be a client delete, if the DCC has causal information 
        % newer than the node clock; or its a normal PUT.
            ok = dotted_db_storage:put(State#state.objects, Key, StrippedDCC)
    end,
    % append the key to the tail of the key log
    {Base, Keys} = State#state.keylog,
    KeyLog = {Base, Keys ++ [Key]},
    % return the updated node state
    {reply, {ok, ReqID, NewDCC}, State#state{clock = NodeClock, keylog = KeyLog}};


handle_command({replicate, ReqID, Key, NewDCC}, _Sender, State) ->
    NodeClock = dcc:add(State#state.clock, NewDCC),
    % get and fill the causal history of the local key
    DiskDCC = guaranteed_get(Key, State),
    % synchronize both objects
    FinalDCC = dcc:sync(NewDCC, DiskDCC),
    % save the new key DCC in the map, while stripping the unnecessary causality
    ok = dotted_db_storage:put(State#state.objects, Key, dcc:strip(FinalDCC, NodeClock)),
    % return the updated node state
    {reply, {ok, ReqID}, State#state{clock = NodeClock}};




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({sync_start, ReqID}, _Sender, State) ->
    % get this node's peers, i.e., all nodes that replicates any subset of local keys
    Peers = dotted_db_utils:peers(State#state.index),
    % choose a random node from that list
    Peer = {Index,_Node} = dotted_db_utils:random_from_list(Peers),
    % get the "Peer"'s entry from this node clock 
    RemoteEntry = bvv:get(Index, State#state.clock),
    % get stats to return to the Sync FSM: {node a, node b}
    Stats = {{State#state.id, node()}, Peer},
    %send a sync message to that node
    {reply, {ok, ReqID, Peer, State#state.id, RemoteEntry, Stats}, State};


handle_command({sync_request, ReqID, RemoteID, RemoteEntry={Base,_Dots}}, _Sender, State) ->
    % get the all the dots (only the counters) from the local node clock, with id equal to the local node
    LocalDots = bvv:values(bvv:get(State#state.id, State#state.clock)),
    % get the all the dots (only the counters) from the asking node clock, with id equal to the local node
    RemoteDots =  bvv:values(RemoteEntry),
    % calculate what dots are present locally that the asking node does not have
    MisssingDots = LocalDots -- RemoteDots,
    {KBase, KeyList} = State#state.keylog,
    % get the keys corresponding to the missing dots,
    MissingKeys = [lists:nth(MDot-KBase, KeyList) || MDot <- MisssingDots],
    % filter the keys that the asking node does not replicate
    RelevantMissingKeys = [Key || Key <- MissingKeys, 
                            lists:member(RemoteID, dotted_db_utils:replica_nodes_indices(Key))],
    % get each key's respective DCC
    RelevantMissingObjects = [{Key, guaranteed_get(Key, State)} || Key <- RelevantMissingKeys],
    % strip any unnecessary causal information to save network bandwidth
    StrippedObjects = [{Key, dcc:strip(DCC, State#state.clock)} || {Key,DCC} <- RelevantMissingObjects],
    % update the replicated clock to reflect what the asking node has about the local node
    Replicated = vv:add(State#state.replicated, {RemoteID, Base}),
    % get that maximum dot generated at this node that is also known by all peers of this node (relevant nodes) 
    MinimumDot = vv:min(Replicated),
    % remove the keys from the keylog that have a dot, corresponding to their position, smaller than the
    % minimum dot, i.e., this update is known by all nodes that replicate it and therefore can be removed
    % form the keylog; for simplicity, remove only keys that start at the head, to actually shrink the log
    % and increment the base counter.
    {RemovedKeys, KeyLog} =
        case MinimumDot > KBase of
            false -> % we don't need to remove any keys from the log
                {[], {KBase, KeyList}};
            true  -> % we can remove keys and shrink the keylog
                {RemKeys, CurrentKeys} = lists:split(MinimumDot - KBase, KeyList),
                {RemKeys, {MinimumDot, CurrentKeys}}
        end,
    % take this opportunity to revisit the removed keys from the keylog and try to strip them of their
    % current causal information; the goal is to removed all causal information (the VV in the DCC),
    % except the single dot for every concurrent value in the object.
    LocalStrippedObjects = [{Key, guaranteed_get(Key, State)} || Key <- RemovedKeys],
    % save the stripped versions of the keys that were removed from the keylog
    [dotted_db_storage:put(State#state.objects, Key, dcc:strip(DCC, State#state.clock)) 
        || {Key, DCC} <- LocalStrippedObjects],
    % get stats to return to the Sync FSM: {replicated vv, keylog, keylog length, b2a_number, b2a_size, b2a_size_full}
    FilledObjects = [{Key, dcc:fill(DCC, State#state.clock)} || {Key,DCC} <- RelevantMissingObjects],
    {B1,K1} = KeyLog,
    Stats = {
        size(term_to_binary(Replicated)),
        size(term_to_binary(KeyLog)),
        length(K1) + B1,
        length(StrippedObjects),
        size(term_to_binary(StrippedObjects)),
        size(term_to_binary(FilledObjects))
    },
    % send the final objects and the base (contiguous) dots of the node clock to the asking node
    {reply, {ok, ReqID, State#state.id, bvv:base(State#state.clock), StrippedObjects, Stats},
        State#state{replicated = Replicated, keylog = KeyLog}};

handle_command({sync_response, ReqID, RespondingNodeID, RemoteNodeClockBase, MissingObjects}, _Sender, State) ->
    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    RemoteEntry = bvv:get(RespondingNodeID, RemoteNodeClockBase),
    NodeClock = bvv:store_entry(RespondingNodeID, RemoteEntry, State#state.clock),
    % get the local objects corresponding to the received objects and fill the 
    % causal history for all of them
    FilledObjects =
        [{ Key, dcc:fill(DCC, RemoteNodeClockBase), guaranteed_get(Key, State) } 
         || {Key,DCC} <- MissingObjects],
    % synchronize / merge the remote and local objects
    SyncedObjects = [{ Key, dcc:sync(Remote, Local) } || {Key, Remote, Local} <- FilledObjects],
    % save the synced objects and strip their causal history
    [dotted_db_storage:put(State#state.objects, Key, dcc:strip(DCC, State#state.clock)) 
        || {Key, DCC} <- SyncedObjects],
    {reply, {ok, ReqID}, State#state{clock = NodeClock}};


%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.id}, State};

handle_command(Message, _Sender, State) ->
    lager:warning({unhandled_command, Message}),
    {noreply, State}.


%%%===================================================================
%%% HANDOFF 
%%%===================================================================

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dotted_db_storage:fold(State#state.objects, Fun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Key, Obj} = binary_to_term(Data),
    NewObj = guaranteed_get(Key, State),
    FinalObj = dcc:sync(Obj, NewObj),
    ok = dotted_db_storage:put(State#state.objects, Key, dcc:strip(FinalObj, State#state.clock)),
    {reply, ok, State}.

encode_handoff_item(Key, Val) ->
    term_to_binary({Key,Val}).

is_empty(State) ->
    Bool = dotted_db_storage:is_empty(State#state.objects),
    {Bool, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the value (DCC) associated with the Key. 
% By default, we want to return a filled causality, unless we get a storage error.
% If the key does not exists or for some reason, the storage returns an 
% error, return an empty DCC (also filled).
guaranteed_get(Key, State) ->
    case dotted_db_storage:get(State#state.objects, Key) of
        {error, not_found} -> 
            % there is no key K in this node
            dcc:fill(dcc:new(), State#state.clock);
        {error, Error} -> 
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            dcc:new();
        DCC -> 
            % get and fill the causal history of the local object
            dcc:fill(DCC, State#state.clock)
    end.


