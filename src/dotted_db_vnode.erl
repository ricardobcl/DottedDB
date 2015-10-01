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
         handle_exit/3
        ]).

-export([
         read/3,
         repair/3,
         write/6,
         replicate/4,
         sync_start/2,
         sync_missing/4,
         sync_repair/5
        ]).

-ignore_xref([
             start_vnode/1
             ]).

-type dets() :: reference().

-record(state, {
        % node id used for in logical clocks
        id          :: id(),
        % index on the consistent hashing ring
        index       :: index(),
        % the current node pid
        node        :: node(),
        % node logical clock
        clock       :: bvv(),
        % key->value store, where the value is a DCC (values + logical clock)
        storage     :: dotted_db_storage:storage(),
        % what peer nodes have from my coordinated writes (not real-time)
        replicated  :: vv(),
        % log for keys that this node coordinated a write (eventually older keys are safely pruned)
        keylog      :: keylog(),
        % number of updates (put or deletes) since saving node state to storage
        updates_mem :: integer(),
        % DETS table that stores in disk the vnode state
        dets        :: dets(),
        % a flag to collect or not stats
        stats       :: boolean(),
        % syncs stats
        syncs       :: [{id(), integer(), integer(), os:timestamp(), os:timestamp()}]
    }).

-type state() :: #state{}.

-define(MASTER, dotted_db_vnode_master).
-define(UPDATE_LIMITE, 100). % save vnode state every 100 updates
-define(REPORT_TICK_INTERVAL, 500). % interval between report stats
-define(BUFFER_STRIP_INTERVAL, 200). % interval between attempts to strip local keys (includes replicated keys)
-define(VNODE_STATE_FILE, "dotted_db_vnode_state").
-define(VNODE_STATE_KEY, "dotted_db_vnode_state_key").

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

repair(OutdatedNodes, BKey, DCC) ->
    riak_core_vnode_master:command(OutdatedNodes,
                                   {repair, BKey, DCC},
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

sync_missing(Peer, ReqID, RemoteNodeID, RemoteEntry) ->
    riak_core_vnode_master:command(Peer,
                                   {sync_missing, ReqID, RemoteNodeID, RemoteEntry},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_repair(Node, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects) ->
    riak_core_vnode_master:command(Node,
                                   {sync_repair, ReqID, RemoteNodeID, RemoteNodeClockBase, MissingObjects},
                                   {fsm, undefined, self()},
                                   ?MASTER).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([Index]) ->
    lager:debug("INITS: {~p, ~p}",[Index,node()]),
    % try to read the vnode state in the DETS file, if it exists
    {Dets, NodeClock, KeyLog, Replicated} =
        case read_vnode_state(Index) of
            {Ref, not_found} -> % there isn't a past vnode state stored
                lager:debug("No persisted state for vnode ~p.",[Index]),
                Clock = bvv:new(),
                KLog  = {0,[]},
                Repli = initialize_replicated(Index),
                {Ref, Clock, KLog, Repli};
            {Ref, error, Error} -> % some unexpected error
                lager:error("Error reading vnode state from storage: ~p", [Error]),
                % TODO: ideally, we should use a new vnode id.....
                Clock = bvv:new(),
                KLog  = {0,[]},
                Repli = initialize_replicated(Index),
                {Ref, Clock, KLog, Repli};
            {Ref, {Clock, KLog, Repli}} -> % we have vnode state in the storage
                lager:debug("Recovered state for vnode ~p.",[Index]),
                {Ref, Clock, KLog, Repli}
        end,
    % open the storage backend for the key-values of this vnode
    {Storage, NodeClock2, KeyLog2, Replicated2} =
        case open_storage(Index) of
            {{backend, ets}, S} ->
                % if the storage is in memory, start with an "empty" vnode state
                {S, bvv:new(), {0,[]}, initialize_replicated(Index)};
            {_, S} ->
                {S, NodeClock, KeyLog, Replicated}
        end,
    % ?PRINT({node(), Index,NodeClock2, KeyLog2, Replicated2} ),
    % create the state
    {ok, #state{
        % for now, lets use the index in the consistent hash as the vnode ID
        id          = Index,
        index       = Index,
        node        = node(),
        clock       = NodeClock2,
        replicated  = Replicated2,
        keylog      = KeyLog2,
        storage     = Storage,
        dets        = Dets,
        updates_mem = 0,
        stats       = true,
        syncs       = initialize_syncs(Index)
        }
    }.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({read, ReqID, Key}, _Sender, State) ->
    Response =
        case dotted_db_storage:get(State#state.storage, Key) of
            {error, not_found} ->
                % there is no key K in this node
                % create an empty "object" and fill its causality with the node clock
                % this is needed to ensure that deletes "win" over old writes at the coordinator
                {ok, fill_clock(Key, dcc:new(), State#state.clock)};
            {error, Error} ->
                % some unexpected error
                lager:error("Error reading a key from storage (command read): ~p", [Error]),
                % return the error
                {error, Error};
            DCC ->
                % get and fill the causal history of the local object
                {ok, fill_clock(Key, DCC, State#state.clock)}
        end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    IndexNode = {State#state.index, State#state.node},
    {reply, {ok, ReqID, IndexNode, Response}, State};


handle_command({repair, BKey, NewDCC}, Sender, State) ->
    {reply, {ok, dummy_req_id}, State2} =
        handle_command({replicate, dummy_req_id, BKey, NewDCC}, Sender, State),
    {noreply, State2};



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({write, ReqID, Operation, Key, Value, Context}, _Sender, State) ->

    % debug
    RN = dotted_db_utils:replica_nodes(Key),
    This = {State#state.id, node()},
    case lists:member(This, RN) of
        true   ->
            ok;
        false ->
            lager:info("WRONG NODE!!!! IxNd: ~p work vnode for key ~p in ~p", [This, Key, RN])
    end,

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
            ok = dotted_db_storage:delete(State#state.storage, Key);
        false -> % we still have relevant information (PUT or DELETE).
        % this can still be a client delete, if the DCC has causal information
        % newer than the node clock; or its a normal PUT.
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC)
    end,
    % append the key to the tail of the key log
    {Base, Keys} = State#state.keylog,
    KeyLog = {Base, Keys ++ [Key]},
    % increment the updates since saving
    UpdatesMemory =  case State#state.updates_mem =< ?UPDATE_LIMITE of
        true -> % it's still early to save to storage
            State#state.updates_mem + 1;
        false ->
            % it's time to persist vnode state
            save_vnode_state(State#state.dets, State#state.id, {NodeClock, KeyLog, State#state.replicated}),
            % restart the counter
            0
    end,
    % Optionally collect stats
    case State#state.stats of
        true ->
            % dotted_db_stats:notify({histogram, bvv_size}, size(term_to_binary(NodeClock))),
            % {_, List1} = KeyLog,
            % dotted_db_stats:notify({histogram, kl_len}, length(List1)),

            % MetaF = byte_size(term_to_binary(dcc:context(NewDCC))),
            % MetaS = byte_size(term_to_binary(dcc:context(StrippedDCC))),
            % CCF = length(dcc:context(NewDCC)),
            % CCS = length(dcc:context(StrippedDCC)),
            % dotted_db_stats:update_key_meta(State#state.index, 1, MetaF, MetaS, CCF, CCS),

            ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID, NewDCC},
        State#state{clock = NodeClock, keylog = KeyLog, updates_mem = UpdatesMemory}};


handle_command({replicate, ReqID, Key, NewDCC}, _Sender, State) ->


% debug
    RN = dotted_db_utils:replica_nodes(Key),
    This = {State#state.id, node()},
    case lists:member(This, RN) of
        true   ->
            ok;
        false ->
            lager:info("WRONG NODE!!! (2)IxNd: ~p work vnode for key ~p in ~p", [This, Key, RN])
    end,



    NodeClock = dcc:add(State#state.clock, NewDCC),
    % get and fill the causal history of the local key
    DiskDCC = guaranteed_get(Key, State),
    % synchronize both objects
    FinalDCC = dcc:sync(NewDCC, DiskDCC),
    % test if the FinalDCC has newer information
    case FinalDCC == DiskDCC of
        true -> 
            lager:debug("Replicated object is ignored (already seen)");
        false ->
            % strip the causality
            StrippedDCC = dcc:strip(FinalDCC, NodeClock),
            % save the new key DCC, while stripping the unnecessary causality
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC)
    end,
    % Optionally collect stats
    case State#state.stats andalso FinalDCC =/= DiskDCC of
        true ->
            % StrippedDCC2 = dcc:strip(FinalDCC, NodeClock),
            % MetaF = byte_size(term_to_binary(dcc:context(FinalDCC))),
            % MetaS = byte_size(term_to_binary(dcc:context(StrippedDCC2))),
            % CCF = length(dcc:context(FinalDCC)),
            % CCS = length(dcc:context(StrippedDCC2)),
            % dotted_db_stats:update_key_meta(State#state.index, 1, MetaF, MetaS, CCF, CCS),
            ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID}, State#state{clock = NodeClock}};




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({sync_start, ReqID}, _Sender, State) ->
    % choose a peer at random
    NodeB = {IndexB, _} = dotted_db_utils:random_from_list(dotted_db_utils:peers(State#state.id)),
    % get the NodeB entry from this node clock
    EntryB = bvv:get(IndexB, State#state.clock),
    % update this node sync stats
    NewLastAttempt = os:timestamp(),
    Fun = fun ({PI, ToCounter, FromCounter, LastAttempt, LastExchange}) ->
            case PI =:= IndexB of
                true -> {PI, ToCounter + 1, FromCounter, NewLastAttempt, LastExchange};
                false -> {PI, ToCounter, FromCounter, LastAttempt, LastExchange}
            end
          end,
    Syncs = lists:map(Fun, State#state.syncs),
    % send a sync message to that node
    {reply, {ok, ReqID, State#state.id, NodeB, EntryB}, State#state{syncs = Syncs}};


handle_command({sync_missing, ReqID, RemoteID, LocalEntryInRemoteClock}, _Sender, State) ->
    % get the all the dots (only the counters) from the local node clock, with id equal to the local node
    LocalDots = bvv:values(bvv:get(State#state.id, State#state.clock)),
    % get the all the dots (only the counters) from the asking node clock, with id equal to the local node
    RemoteDots =  bvv:values(LocalEntryInRemoteClock),
    % calculate what dots are present locally that the asking node does not have
    MisssingDots = LocalDots -- RemoteDots,
    {KBase, KeyList} = State#state.keylog,
    % get the keys corresponding to the missing dots,
    MissingKeys = [lists:nth(MDot-KBase, KeyList) || MDot <- MisssingDots, MDot > KBase],
    % filter the keys that the asking node does not replicate
    RelevantMissingKeys = [Key || Key <- MissingKeys,
                            lists:member(RemoteID, dotted_db_utils:replica_nodes_indices(Key))],
    % get each key's respective DCC
    RelevantMissingObjects = [{Key, guaranteed_get(Key, State)} || Key <- RelevantMissingKeys],
    % strip any unnecessary causal information to save network bandwidth
    StrippedObjects = [{Key, dcc:strip(DCC, State#state.clock)} || {Key,DCC} <- RelevantMissingObjects],
    % debug
    case orddict:find(RemoteID, State#state.replicated) of
        error   ->
            lager:info("IxNd: ~p new entry in replicated VV for ~p", [{State#state.id, node()}, RemoteID]);
        {ok, _} ->
            ok
    end,
    % Optionally collect stats
    case State#state.stats of
        true ->
            Ratio_Relevant_Keys = round(100*length(RelevantMissingKeys)/max(1,length(MissingKeys))),
            case MissingKeys > 0 of
                true -> dotted_db_stats:notify({histogram, sync_relevant_ratio}, Ratio_Relevant_Keys);
                false -> ok
            end,

            Ctx_Sent_Strip = [dcc:context(DCC) || {_Key, DCC} <- StrippedObjects],
            Sum_Ctx_Sent_Strip = lists:sum([length(DCC) || DCC <- Ctx_Sent_Strip]),
            Ratio_Sent_Strip = Sum_Ctx_Sent_Strip/max(1,length(StrippedObjects)),
            case Ratio_Sent_Strip =:= 0.0 orelse length(StrippedObjects) == 0 of
                true -> ok;
                false -> dotted_db_stats:notify({histogram, sync_sent_dcc_strip}, Ratio_Sent_Strip)
            end,

            Size_Meta_Sent = byte_size(term_to_binary(Ctx_Sent_Strip)),
            dotted_db_stats:notify({histogram, sync_metadata_size}, Size_Meta_Sent),

            Payload_Sent_Strip = [{Key, dcc:values(DCC)} || {Key, DCC} <- StrippedObjects],
            Size_Payload_Sent = byte_size(term_to_binary(Payload_Sent_Strip)),
            dotted_db_stats:notify({histogram, sync_payload_size}, Size_Payload_Sent),

            ok;
        false -> ok
    end,
    % update this node sync stats
    NewLastAttempt = os:timestamp(),
    Fun = fun ({PI, ToCounter, FromCounter, LastAttempt, LastExchange}) ->
            case PI =:= RemoteID of
                true -> {PI, ToCounter, FromCounter + 1, NewLastAttempt, LastExchange};
                false -> {PI, ToCounter, FromCounter, LastAttempt, LastExchange}
            end
          end,
    Syncs = lists:map(Fun, State#state.syncs),
    % send the final objects and the base (contiguous) dots of the node clock to the asking node
    {reply, {   ok,
                ReqID,
                State#state.id,
                bvv:base(State#state.clock),
                bvv:get(RemoteID, State#state.clock), % Remote entry in this node's global clock
                StrippedObjects},
        State#state{syncs = Syncs}};

handle_command({sync_repair, ReqID, RemoteNodeID, RemoteClockBase, MissingObjects}, _Sender, State) ->
    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    RemoteEntry = bvv:get(RemoteNodeID, RemoteClockBase),
    NodeClock = bvv:store_entry(RemoteNodeID, RemoteEntry, State#state.clock),
    % get the local objects corresponding to the received objects and fill the
    % causal history for all of them
    FilledObjects =
        [{ Key, fill_clock(Key, DCC, RemoteClockBase), guaranteed_get(Key, State) }
         || {Key,DCC} <- MissingObjects],
    % synchronize / merge the remote and local objects
    SyncedObjects = [{ Key, dcc:sync(Remote, Local), Local } || {Key, Remote, Local} <- FilledObjects],
    % filter the objects that are not missing after all
    RealMissingObjects = [{ Key, Synced } || {Key, Synced, Local} <- SyncedObjects, Synced =/= Local],
    % save the synced objects and strip their causal history
    RealMissingObjects2 = [{Key, DCC, save_kv(Key, DCC, State#state{clock=NodeClock})} || {Key, DCC} <- RealMissingObjects],
    % filter the keys that were totally stripped
    NonStrippedObjects = [{Key, DCC} || {Key, DCC, N} <- RealMissingObjects2, N =/= 0],
    % schedule a later strip attempt for non-stripped synced keys
    NSK = add_keys_to_strip_schedule_objects(NonStrippedObjects, RemoteNodeID, State#state.non_stripped_keys),
    % Garbage Collect keys from the KeyLog and delete keys with no causal context
    State2 = gc_keylog(State#state{clock=NodeClock, non_stripped_keys=NSK}, RemoteNodeID, RemoteClockBase),
    % Optionally collect stats
    Syncs = case State2#state.stats of
        true ->
            Meta = [ dcc:context(DCC)   || {_Key, DCC} <- MissingObjects],
            Payload = [ dcc:values(DCC) || {_Key, DCC} <- MissingObjects],
            MetaSize = byte_size(term_to_binary(Meta)) + byte_size(term_to_binary(RemoteClockBase)),
            PayloadSize = byte_size(term_to_binary(Payload)),
            _Size2 = dotted_db_utils:human_filesize(MetaSize),
            % if
            %     length(MissingObjects) > 0 -> 
            %         lager:info("MissingObjects: ~p    E.bytes: ~s~n", [length(MissingObjects), Size2]);
            %     true -> ok
            % end,
            Repaired = length(RealMissingObjects),
            Sent = length(MissingObjects),
            dotted_db_stats:sync_complete(State2#state.id, Repaired, Sent, {PayloadSize, MetaSize}),

            Hit_Ratio = 100*Repaired/max(1, Sent),
            case Hit_Ratio =:= 0.0 orelse length(MissingObjects) == 0 of
                true -> ok;
                false -> dotted_db_stats:notify({histogram, sync_hit_ratio}, Hit_Ratio)
            end,

            % update this node sync stats
            NewLastExchange = os:timestamp(),
            Fun = fun ({PI, ToCounter, FromCounter, LastAttempt, LastExchange}) ->
                    case PI =:= RemoteNodeID of
                        true -> {PI, ToCounter, FromCounter, LastAttempt, NewLastExchange};
                        false -> {PI, ToCounter, FromCounter, LastAttempt, LastExchange}
                    end
                  end,
            lists:map(Fun, State2#state.syncs);
        false ->
            State2#state.syncs
    end,
    {reply, {ok, ReqID}, State2#state{syncs = Syncs}};


%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.id}, State};

handle_command(get_vnode_state, _Sender, State) ->
    {reply, {pong, State}, State};

handle_command(Message, _Sender, State) ->
    lager:warning({unhandled_command, Message}),
    {noreply, State}.


%%%===================================================================
%%% Coverage
%%%===================================================================

handle_coverage(vnode_state, _KeySpaces, {_, RefId, _}, State) ->
    {_,K} = State#state.keylog,
    KL = {length(K), byte_size(term_to_binary(State#state.keylog))},
    {reply, {RefId, {ok, State#state{keylog = KL } }}, State};

% handle_coverage({list_streams, Username}, _KeySpaces, {_, RefId, _}, State) ->
%     Streams = lists:sort(list_streams(State, Username)),
%     {reply, {RefId, {ok, Streams}}, State};

% handle_coverage(list_users, _KeySpaces, {_, RefId, _}, State) ->
%     Users = lists:sort(list_users(State)),
%     {reply, {RefId, {ok, Users}}, State};

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    % lager:warning("unknown coverage received ~p", [Req]),
    lager:info("unknown coverage received ~p", [Req]),
    {noreply, State}.


%%%===================================================================
%%% HANDOFF
%%%===================================================================

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender, State) ->
    % we need to wrap the fold function because it expect 3 elements (K,V,Acc),
    % and our storage layer expect 2 elements ({K,V},Acc).
    WrapperFun = fun({Key,Val}, Acc) -> FoldFun(Key, Val, Acc) end,
    Acc = dotted_db_storage:fold(State#state.storage, WrapperFun, Acc0),
    {reply, Acc, State};

%% Ignore AAE sync requests
handle_handoff_command(Cmd, _Sender, State) when
        element(1, Cmd) == sync_start orelse
        element(1, Cmd) == sync_request orelse
        element(1, Cmd) == sync_response ->
    {drop, State};

handle_handoff_command(Cmd, Sender, State) when
        element(1, Cmd) == replicate orelse
        element(1, Cmd) == repair ->
    case handle_command(Cmd, Sender, State) of
        {noreply, State2} ->
            {forward, State2};
        {reply, {ok,_}, State2} ->
            {forward, State2}
    end;

%% For coordinating writes, do it locally and forward the replication
handle_handoff_command(Cmd={write, ReqID, _, Key, _, _}, Sender, State) ->
    lager:info("HAND_WRITE: {~p, ~p} // Key: ~p",[State#state.index, node(), Key]),
    % do the local coordinating write
    {reply, {ok, ReqID, NewDCC}, State2} = handle_command(Cmd, Sender, State),
    % send the ack to the PUT_FSM
    riak_core_vnode:reply(Sender, {ok, ReqID, NewDCC}),
    % create a new request to forward the replication of this new DCC/object
    NewCommand = {replicate, ReqID, Key, NewDCC},
    {forward, NewCommand, State2};

%% Handle all other commands locally (only gets?)
handle_handoff_command(Cmd, Sender, State) ->
    lager:info("Handoff command ~p at ~p", [Cmd, State#state.id]),
    handle_command(Cmd, Sender, State).

handoff_starting(TargetNode, State) ->
    lager:info("HAND_START: {~p, ~p} to ~p",[State#state.index, node(), TargetNode]),
    %% save the vnode state, if not empty
    case State#state.clock =:= bvv:new() of
        true -> ok;
        false ->
            Key = {?DEFAULT_BUCKET, {?VNODE_STATE_KEY, State#state.index}},
            NodeState = {State#state.clock, State#state.keylog, State#state.replicated},
            dotted_db_storage:put(State#state.storage, Key, NodeState)
    end,
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    NodeKey = {?DEFAULT_BUCKET, {?VNODE_STATE_KEY, State#state.index}},
    % decode the data received
    NewState = 
        case dotted_db_utils:decode_kv(Data) of
            {NodeKey, {NodeClock, KeyLog, Replicated}} ->
                NodeClock2 = bvv:join(NodeClock, State#state.clock),
                State#state{clock = NodeClock2, keylog = KeyLog, replicated = Replicated};
            {Key, Obj} ->
                {reply, {ok, _}, State2} = handle_command({replicate, dummy_req_id, Key, Obj}, undefined, State),
                State2
        end,
    {reply, ok, NewState}.

encode_handoff_item(Key, Val) ->
    dotted_db_utils:encode_kv({Key,Val}).

is_empty(State) ->
    case dotted_db_storage:is_empty(State#state.storage) of
        true ->
            {true, State};
        false ->
            lager:info("IS_EMPTY: not empty -> {~p, ~p}",[State#state.index, node()]),
            {false, State}
    end.

delete(State) ->
    {Good, Storage1} = 
        case dotted_db_storage:drop(State#state.storage) of
            {ok, Storage} ->
                {true, Storage};
            {error, Reason, Storage} ->
                lager:info("BAD_DROP: {~p, ~p}  Reason: ~p",[State#state.index, node(), Reason]),
                {false, Storage}
        end,
    case State#state.clock =/= [] andalso Good of
        true  -> 
            lager:info("IxNd:~p // Clock:~p // KL:~p // VV:~p",
                [{State#state.index, node()}, State#state.clock, State#state.keylog, State#state.replicated] ),
            lager:info("GOOD_DROP: {~p, ~p}",[State#state.index, node()]);
        false -> ok
    end,
    {ok, State#state{storage=Storage1}}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    lager:debug("HAND_TERM: {~p, ~p}",[State#state.index, node()]),
    close_all(State),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


fill_clock(Key, LocalClock, GlobalClock) ->
    dcc:fill(LocalClock, GlobalClock, dotted_db_utils:replica_nodes_indices(Key)).

% @doc Returns the value (DCC) associated with the Key.
% By default, we want to return a filled causality, unless we get a storage error.
% If the key does not exists or for some reason, the storage returns an
% error, return an empty DCC (also filled).
guaranteed_get(Key, State) ->
    case dotted_db_storage:get(State#state.storage, Key) of
        {error, not_found} ->
            % there is no key K in this node
            fill_clock(Key, dcc:new(), State#state.clock);
        {error, Error} ->
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            dcc:new();
        DCC ->
            % get and fill the causal history of the local object
            fill_clock(Key, DCC, State#state.clock)
    end.

% @doc Saves the relevant vnode state to the storage.
save_vnode_state(Dets, Id, State={_,_,_}) ->
    Key = {?VNODE_STATE_KEY, Id},
    ok = dets:insert(Dets, {Key, State}),
    ok = dets:sync(Dets),
    lager:debug("Saved state for vnode ~p.",[Id]),
    ok.

% @doc Reads the relevant vnode state from the storage.
read_vnode_state(Id) ->
    Folder = "data/vnode_state/",
    ok = filelib:ensure_dir(Folder),
    FileName = filename:join(Folder, integer_to_list(Id)),
    Ref = list_to_atom(integer_to_list(Id)),
    {ok, Dets} = dets:open_file(Ref,[{type, set},
                                    {file, FileName},
                                    {auto_save, infinity},
                                    {min_no_slots, 1}]),
    Key = {?VNODE_STATE_KEY, Id},
    case dets:lookup(Dets, Key) of
        [] -> % there isn't a past vnode state stored
            {Dets, not_found};
        {error, Error} -> % some unexpected error
            {Dets, error, Error};
        [{Key, State={_,_,_}}] ->
            {Dets, State}
    end.

% @doc Initializes the "replicated" version vector to 0 for peers of this vnode.
initialize_replicated(Index) ->
    % get this node's peers, i.e., all nodes that replicates any subset of local keys.
    PeerIDs = [ ID || {ID, _Node} <- dotted_db_utils:peers(Index)],
    % for replication factor N = 3, the numbers of peers should be 4 (2 vnodes before and 2 after).
    (?REPLICATION_FACTOR-1)*2 = length(PeerIDs),
    % initialize the "replicated" version vector to 0 for all entries.
    % this is vital, because we basically care for the minimum value in all entries,
    % thus we require that every node peer must be present from the start.
    Replicated = lists:foldl(fun (ID,VV) -> vv:add(VV,{ID,0}) end , vv:new(), PeerIDs),
    (?REPLICATION_FACTOR-1)*2 = length(Replicated),
    Replicated.


% @doc Initializes the "sync" stats for peers of this vnode.
initialize_syncs(Index) ->
    % get this node's peers, i.e., all nodes that replicates any subset of local keys.
    PeerIDs = [ ID || {ID, _Node} <- dotted_db_utils:peers(Index)],
    % for replication factor N = 3, the numbers of peers should be 4 (2 vnodes before and 2 after).
    (?REPLICATION_FACTOR-1)*2 = length(PeerIDs),
    Now = os:timestamp(),
    Syncs = lists:foldl(fun (ID, List) -> [{ID,0,0,Now,Now} | List] end , [], PeerIDs),
    (?REPLICATION_FACTOR-1)*2 = length(Syncs),
    Syncs.



% @doc Returns the Storage for this vnode.
open_storage(Index) ->
    % get the preferred backend in the configuration file, defaulting to ETS if
    % there is no preference.
    Backend = case app_helper:get_env(dotted_db, storage_backend) of
        "leveldb"   -> {backend, leveldb};
        "ets"       -> {backend, ets};
        _           -> {backend, ets}
    end,
    lager:info("Using ~p for vnode ~p.",[Backend,Index]),
    % give the name to the backend for this vnode using its position in the ring.
    DBName = filename:join("data/objects/", integer_to_list(Index)),
    {ok, Storage} = dotted_db_storage:open(DBName, [Backend]),
    {Backend, Storage}.

% @doc Close the key-value backend, save the vnode state and close the DETS file.
close_all(undefined) -> ok;
close_all(_State=#state{id          = Id,
                        storage     = Storage,
                        clock       = NodeClock,
                        replicated  = Replicated,
                        keylog      = KeyLog,
                        dets        = Dets } ) ->
    case dotted_db_storage:close(Storage) of
        ok -> ok;
        {error, Reason} ->
            lager:warning("Error on closing storage: ~p",[Reason])
    end,
    ok = save_vnode_state(Dets, Id, {NodeClock, KeyLog, Replicated}),
    ok = dets:close(Dets).


gc_keylog(State, RemoteID, RemoteClockBase) ->
    {Base,_} = bvv:get(State#state.id, RemoteClockBase),
    {KBase, KeyList} = State#state.keylog,
    % update the replicated clock to reflect what the asking node has about the local node
    Replicated = vv:add(State#state.replicated, {RemoteID, Base}),
    % get the oldest dot generated at this node that is also known by all peers of this node (relevant nodes)
    MinimumDot = vv:min(Replicated),
    % remove the keys from the keylog that have a dot (corresponding to their position) smaller than the
    % minimum dot, i.e., this update is known by all nodes that replicate it and therefore can be removed
    % from the keylog; for simplicity, remove only keys that start at the head, to actually shrink the log
    % and increment the base counter.
    {RemovedKeys, KeyLog={NewBase,_}} =
        case MinimumDot > KBase of
            false -> % we don't need to remove any keys from the log
                {[], {KBase, KeyList}};
            true  -> % we can remove keys and shrink the keylog
                {RemKeys, CurrentKeys} = lists:split(MinimumDot - KBase, KeyList),
                {RemKeys, {MinimumDot, CurrentKeys}}
        end,
    % add the non stripped keys to the node state for later strip attempt
    NSK = add_keys_to_strip_schedule_keylog(RemovedKeys, State#state.id, NewBase,State#state.non_stripped_keys),
    % Optionally collect stats
    case State#state.stats of
        true ->
            % CCF = length(RemovedKeys) * ?REPLICATION_FACTOR,
            % CCS = lists:sum(NumEntries),
            % EntryExampleSize = byte_size(term_to_binary({State#state.id, 123345})),
            % MetaF = EntryExampleSize * ?REPLICATION_FACTOR * length(RemovedKeys),
            % MetaS = EntryExampleSize * CCS,
            % dotted_db_stats:update_key_meta(State#state.index, length(RemovedKeys), MetaF, MetaS, CCF, CCS),

            % SLDS = CCS/max(1,length(RemovedKeys)),
            % case SLDS =:= 0.0 orelse length(RemovedKeys) == 0 of
            %     true -> ok;
            %     false -> dotted_db_stats:notify({histogram, sync_local_dcc_strip}, SLDS)
            % end,
            ok;
        false -> ok
    end,
    State#state{replicated=Replicated, keylog=KeyLog, non_stripped_keys=NSK}.


save_kv(Key, ValueDCC, State) ->
    save_kv(Key, ValueDCC, State, true).
save_kv(Key, ValueDCC, State, Stats) ->
    % removed unnecessary causality from the DCC, based on the current node clock
    StrippedDCC = dcc:strip(ValueDCC, State#state.clock),
    % the resulting object/DCC is one of the following options:
    %   * it has no value and no causal history -> can be deleted
    %   * it has no value but has causal history -> it's a delete, but still must be persisted
    %   * has values, with causal context -> it's a normal write and we should persist
    %   * has values, but no causal context -> it's the final form for this write
    case StrippedDCC of
        {[],[]} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 1}),
            ok = dotted_db_storage:delete(State#state.storage, Key),
            0;
        {[{_,delete}],[]} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 1}),
            ok = dotted_db_storage:delete(State#state.storage, Key),
            0;
        {[],CC} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 0}),
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC),
            length(CC);
        {[{_,delete}],CC} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 0}),
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC),
            length(CC);
        {_ ,[]} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 3}),
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC),
            0;
        {_ ,CC} ->
            Stats andalso ets:insert(get_ets_id(State#state.id), {Key, 2}),
            ok = dotted_db_storage:put(State#state.storage, Key, StrippedDCC),
            length(CC)
    end.

