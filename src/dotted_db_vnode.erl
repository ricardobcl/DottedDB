-module(dotted_db_vnode).
-behaviour(riak_core_vnode).
-include_lib("dotted_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         handle_info/2,
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
            get_vnode_id/1,
            broadcast_my_peers_to_my_peers/3,
            replace_peer/3,
            restart/2,
            inform_peers_restart/2,
            inform_peers_restart2/2,
            recover_keys/2,
            read/3,
            repair/3,
            write/2,
            replicate/2,
            sync_start/2,
            sync_missing/5,
            sync_repair/2
        ]).

-ignore_xref([
             start_vnode/1
             ]).

-type dets()        :: reference().

-record(state, {
        % node id used for in logical clocks
        id          :: vnode_id(),
        % the atom representing the vnode id
        atom_id     :: atom(),
        % index on the consistent hashing ring
        index       :: index(),
        % my peers ids
        peers_ids   :: [vnode_id()],
        % node logical clock
        clock       :: bvv(),
        % key->object store, where the object contains a DCC (values + logical clock)
        storage     :: dotted_db_storage:storage(),
        % what me and my peers know about me and their peers
        watermark   :: vv_matrix(),
        % map for keys that this node replicates (eventually all keys are safely pruned from this)
        dotkeymap   :: key_matrix(),
        % the left list of pairs of deleted keys not yet stripped, and their causal context (version vector);
        % the right side is a list of (vnode, map), where the map is between dots and keys not yet completely stripped (and their VV also)
        non_stripped_keys :: {[{key(),vv()}], [{id(), dict:dict()}]},
        % interval in which the vnode tries to strip the non-stripped-keys
        buffer_strip_interval :: non_neg_integer(),
        % temporary list of nodes recovering from failure and a list of keys to send
        recover_keys :: [{id(), [bkey()]}],
        % number of updates (put or deletes) since saving node state to storage
        updates_mem :: integer(),
        % DETS table that stores in disk the vnode state
        dets        :: dets(),
        % a flag to collect or not stats
        stats       :: boolean(),
        % syncs stats
        syncs       :: [{id(), integer(), integer(), os:timestamp(), os:timestamp()}],
        % what mode the vnode is on
        mode        :: normal | recovering,
        % interval time between reports on this vnode
        report_interval :: non_neg_integer()
    }).

-type state() :: #state{}.

-define(MASTER, dotted_db_vnode_master).
-define(UPDATE_LIMITE, 100). % save vnode state every 100 updates
-define(VNODE_STATE_FILE, "dotted_db_vnode_state").
-define(VNODE_STATE_KEY, "dotted_db_vnode_state_key").
-define(ETS_DELETE_NO_STRIP, 0).
-define(ETS_DELETE_STRIP,    1).
-define(ETS_WRITE_NO_STRIP,  2).
-define(ETS_WRITE_STRIP,     3).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

get_vnode_id(IndexNodes) ->
    riak_core_vnode_master:command(IndexNodes,
                                   get_vnode_id,
                                   {raw, undefined, self()},
                                   ?MASTER).

broadcast_my_peers_to_my_peers(IndexNodes, MyId, MyPeersIds) ->
    riak_core_vnode_master:command(IndexNodes,
                                   {broadcast_my_peers_to_my_peers, MyId, MyPeersIds},
                                   {raw, undefined, self()},
                                   ?MASTER).

replace_peer(IndexNodes, OldPeerId, NewPeerId) ->
    riak_core_vnode_master:command(IndexNodes,
                                   {replace_peer, OldPeerId, NewPeerId},
                                   {raw, undefined, self()},
                                   ?MASTER).

restart(IndexNodes, ReqID) ->
    riak_core_vnode_master:command(IndexNodes,
                                   {restart, ReqID},
                                   {fsm, undefined, self()},
                                   ?MASTER).

inform_peers_restart(Peers, Args) ->
    riak_core_vnode_master:command(Peers,
                                   {inform_peers_restart, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).

inform_peers_restart2(Peers, Args) ->
    riak_core_vnode_master:command(Peers,
                                   {inform_peers_restart2, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).

recover_keys(Peers, Args) ->
    riak_core_vnode_master:command(Peers,
                                   {recover_keys, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).

read(ReplicaNodes, ReqID, Key) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {read, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

repair(OutdatedNodes, BKey, Object) ->
    riak_core_vnode_master:command(OutdatedNodes,
                                   {repair, BKey, Object},
                                   {fsm, undefined, self()},
                                   ?MASTER).

write(Coordinator, Args) ->
    riak_core_vnode_master:command(Coordinator,
                                   {write, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).

replicate(ReplicaNodes, Args) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {replicate, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_start(Node, ReqID) ->
    riak_core_vnode_master:command(Node,
                                   {sync_start, ReqID},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_missing(Peer, ReqID, RemoteNodeID, RemoteClock, RemotePeers) ->
    riak_core_vnode_master:command(Peer,
                                   {sync_missing, ReqID, RemoteNodeID, RemoteClock, RemotePeers},
                                   {fsm, undefined, self()},
                                   ?MASTER).

sync_repair(Node, Args) ->
    riak_core_vnode_master:command(Node,
                                   {sync_repair, Args},
                                   {fsm, undefined, self()},
                                   ?MASTER).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([Index]) ->
    put(watermark, false),
    process_flag(priority, high),
    % try to read the vnode state in the DETS file, if it exists
    {Dets, NodeId2, NodeClock, DotKeyMap, Watermark, NonStrippedKeys} =
        case read_vnode_state(Index) of
            {Ref, not_found} -> % there isn't a past vnode state stored
                lager:debug("No persisted state for vnode index: ~p.",[Index]),
                NodeId = new_vnode_id(Index),
                Clock = swc_node:new(),
                KLog  = swc_dotkeymap:new(),
                Repli = swc_watermark:new(),
                {Ref, NodeId, Clock, KLog, Repli, {[],[]}};
            {Ref, error, Error} -> % some unexpected error
                lager:error("Error reading vnode state from storage: ~p", [Error]),
                NodeId = new_vnode_id(Index),
                Clock = swc_node:new(),
                KLog  = swc_dotkeymap:new(),
                Repli = swc_watermark:new(),
                {Ref, NodeId, Clock, KLog, Repli, {[],[]}};
            {Ref, {Id, Clock, DKMap, Repli, NSK}} -> % we have vnode state in the storage
                lager:info("Recovered state for vnode ID: ~p.",[Id]),
                {Ref, Id, Clock, DKMap, Repli, NSK}
        end,
    % open the storage backend for the key-values of this vnode
    {Storage, NodeId3, NodeClock2, DotKeyMap2, Watermark2, NonStrippedKeys2} =
        case open_storage(Index) of
            {{backend, ets}, S} ->
                % if the storage is in memory, start with an "empty" vnode state
                NodeId4 = new_vnode_id(Index),
                {S, NodeId4, swc_node:new(), swc_dotkeymap:new(), swc_watermark:new(), {[],[]}};
            {_, S} ->
                {S, NodeId2,NodeClock, DotKeyMap, Watermark, NonStrippedKeys}
        end,
    PeersIDs = ordsets:del_element(NodeId3, ordsets:from_list(swc_watermark:peers(Watermark2))),
    % create an ETS to store keys written and deleted in this node (for stats)
    AtomID = create_ets_all_keys(NodeId3),
    % schedule a periodic reporting message (wait 2 seconds initially)
    schedule_report(2000),
    % schedule a periodic strip of local keys
    schedule_strip_keys(2000),
    {ok, #state{
        % for now, lets use the index in the consistent hash as the vnode ID
        id                      = NodeId3,
        atom_id                 = AtomID,
        index                   = Index,
        peers_ids               = PeersIDs,
        clock                   = NodeClock2,
        watermark               = Watermark2,
        dotkeymap               = DotKeyMap2,
        non_stripped_keys       = NonStrippedKeys2,
        buffer_strip_interval   = ?BUFFER_STRIP_INTERVAL,
        recover_keys            = [],
        storage                 = Storage,
        dets                    = Dets,
        updates_mem             = 0,
        stats                   = application:get_env(dotted_db, do_stats, ?DEFAULT_DO_STATS),
        syncs                   = initialize_syncs(Index),
        mode                    = normal,
        report_interval         = ?REPORT_TICK_INTERVAL
        }
    }.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(Cmd={read, _ReqID, _Key}, _Sender, State) ->
    handle_read(Cmd, State);

handle_command({repair, BKey, Object}, Sender, State) ->
    {noreply, State2} =
        handle_command({replicate, {dummy_req_id, BKey, Object, ?DEFAULT_NO_REPLY}}, Sender, State),
    {noreply, State2};


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(Cmd={write, _Args}, _Sender, State) ->
    handle_write(Cmd, State);

handle_command(Cmd={replicate, _Args}, _Sender, State) ->
    handle_replicate(Cmd, State);


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(Cmd={sync_start, _ReqID}, _Sender, State) ->
    handle_sync_start(Cmd, State);

handle_command(Cmd={sync_missing, _ReqID, _RemoteID, _RemoteClock, _RemotePeers}, Sender, State) ->
    handle_sync_missing(Cmd, Sender, State);

handle_command(Cmd={sync_repair, _Args}, _Sender, State) ->
    handle_sync_repair(Cmd, State);


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Restarting Vnode (and recovery of keys)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% On the restarting node
handle_command(Cmd={restart, _ReqID}, _Sender, State) ->
    handle_restart(Cmd, State);

%% On the good nodes
handle_command(Cmd={inform_peers_restart, {_ReqID, _RestartingNodeIndex, _OldVnodeID, _NewVnodeID}}, _Sender, State) ->
    handle_inform_peers_restart(Cmd, State);

%% On the restarting node
handle_command(Cmd={recover_keys, {_ReqID, _RemoteVnode, _RemoteVnodeId, _RemoteClock, _Objects, _RemoteWatermark, _LastBatch}}, _Sender, State) ->
    handle_recover_keys(Cmd, State);

%% On the good nodes
handle_command(Cmd={inform_peers_restart2, {_ReqID, _NewVnodeID}}, _Sender, State) ->
    handle_inform_peers_restart2(Cmd, State);

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.id}, State};

handle_command(get_vnode_state, _Sender, State) ->
    {reply, {pong, State}, State};

handle_command({set_strip_interval, NewStripInterval}, _Sender, State) ->
    OldStripInterval = State#state.buffer_strip_interval,
    lager:info("Strip Interval => from: ~p \t to: ~p",[OldStripInterval,NewStripInterval]),
    {noreply, State#state{buffer_strip_interval=NewStripInterval}};

handle_command({set_stats, NewStats}, _Sender, State) ->
    OldStats = State#state.stats,
    lager:info("Vnode stats => from: ~p \t to: ~p",[OldStats, NewStats]),
    {noreply, State#state{stats=NewStats}};

handle_command(get_vnode_id, _Sender, State) ->
    {reply, {get_vnode_id, {State#state.index, node()}, State#state.id}, State};

handle_command({broadcast_my_peers_to_my_peers, MyPeer, MyPeerPeers}, _Sender, State) ->
    Watermark = swc_watermark:add_peer(State#state.watermark, MyPeer, MyPeerPeers),
    case length(swc_watermark:peers(Watermark)) == (?REPLICATION_FACTOR*2)-1 of
        true ->
            put(watermark, true),
            lager:info("Peers 2 peers 4 watermark -> DONE!!!");
        false ->
            % lager:info("Getting my peer's peers ~p/~p",[orddict:size(Watermark), (?REPLICATION_FACTOR*2)-1]),
            ok
    end,
    {noreply, State#state{watermark=Watermark}};

handle_command({replace_peer, OldPeerId, NewPeerId}, _Sender, State) ->
    NewPeersIds =
        case ordsets:is_element(OldPeerId, State#state.peers_ids) of
            true  -> ordsets:add_element(NewPeerId, ordsets:del_element(OldPeerId, State#state.peers_ids));
            false -> State#state.peers_ids
        end,
    % NewWatermark = swc_watermark:replace_peer(State#state.watermark, OldPeerId, NewPeerId),
    add_removed_vnode_jump_clock(OldPeerId),
    NewWatermark = swc_watermark:retire_peer(State#state.watermark, OldPeerId, NewPeerId),
    {noreply, State#state{peers_ids=NewPeersIds, watermark=NewWatermark}};

handle_command(Message, _Sender, State) ->
    lager:info("Unhandled Command ~p", [Message]),
    {noreply, State}.


%%%===================================================================
%%% Coverage
%%%===================================================================

handle_coverage(vnode_state, _KeySpaces, {_, RefId, _}, State) ->
    {reply, {RefId, {ok, vs, State}}, State};

handle_coverage(strip_latency, _KeySpaces, {_, RefId, _}, State) ->
    Latencies = compute_strip_latency(State#state.atom_id),
    {reply, {RefId, {ok, strip_latency, Latencies}}, State};

handle_coverage(replication_latency, _KeySpaces, {_, RefId, _}, State) ->
    Latencies = compute_replication_latency(State#state.atom_id),
    {reply, {RefId, {ok, replication_latency, Latencies}}, State};

handle_coverage(all_current_dots, _KeySpaces, {_, RefId, _}, State) ->
    % Dots = ets_get_all_dots(State#state.atom_id),
    Dots = storage_get_all_dots(State#state.storage),
    {reply, {RefId, {ok, all_current_dots, Dots}}, State};

handle_coverage(actual_deleted_keys, _KeySpaces, {_, RefId, _}, State) ->
    ADelKeys = ets_get_actual_deleted(State#state.atom_id),
    {reply, {RefId, {ok, adk, ADelKeys}}, State};

handle_coverage(issued_deleted_keys, _KeySpaces, {_, RefId, _}, State) ->
    IDelKeys = ets_get_issued_deleted(State#state.atom_id),
    Res = case length(IDelKeys) > 0 of
        true ->
            Key = hd(IDelKeys),
            case dotted_db_storage:get(State#state.storage, Key) of
                {error, not_found} ->
                    % there is Key was deleted locally (improbable, since there was a 0 in the ETS)
                    {Key, not_found};
                {error, Error} ->
                    % some unexpected error
                    lager:error("Error reading a key from storage: ~p", [Error]),
                    % assume that the key was lost, i.e. it's equal to not_found
                    {Key, storage_error};
                Obj ->
                    % save the new k\v and remove unnecessary causal information
                    {Key, dotted_db_object:strip(State#state.clock, Obj), Obj}
            end;
        false ->
            {}
    end,
    ThisVnode = {State#state.index, node()},
    {reply, {RefId, {ok, idk, IDelKeys, Res, ThisVnode}}, State};

handle_coverage(written_keys, _KeySpaces, {_, RefId, _}, State) ->
    WrtKeys = ets_get_issued_written(State#state.atom_id),
    Res = case length(WrtKeys) > 0 of
        true ->
            Key = hd(WrtKeys),
            case dotted_db_storage:get(State#state.storage, Key) of
                {error, not_found} ->
                    % there is Key was deleted locally (improbable, since there was a 0 in the ETS)
                    {Key, not_found};
                {error, Error} ->
                    % some unexpected error
                    lager:error("Error reading a key from storage: ~p", [Error]),
                    % assume that the key was lost, i.e. it's equal to not_found
                    {Key, storage_error};
                Obj ->
                    % save the new k\v and remove unnecessary causal information
                    {Key, dotted_db_object:strip(State#state.clock, Obj), Obj}
            end;
        false ->
            {}
    end,
    ThisVnode = {State#state.index, node()},
    {reply, {RefId, {ok, wk, WrtKeys, Res, ThisVnode}}, State};

handle_coverage(final_written_keys, _KeySpaces, {_, RefId, _}, State) ->
    WrtKeys = ets_get_final_written(State#state.atom_id),
    {reply, {RefId, {ok, fwk, WrtKeys}}, State};

handle_coverage(all_keys, _KeySpaces, {_, RefId, _}, State) ->
    IDelKeys = ets_get_issued_deleted(State#state.atom_id),
    IWrtKeys = ets_get_issued_written(State#state.atom_id),
    FWrtKeys = ets_get_final_written(State#state.atom_id),
    {reply, {RefId, {ok, ak, IDelKeys, IWrtKeys, FWrtKeys}}, State};

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    % lager:warning("unknown coverage received ~p", [Req]),
    lager:info("unknown coverage received ~p", [Req]),
    {noreply, State}.


handle_info({undefined,{get_vnode_id, IndexNode={Index,_}, PeerId={Index,_}}}, State) ->
    % lager:info("New vnode id for watermark: ~p ", [PeerId]),
    case lists:member(IndexNode, dotted_db_utils:peers(State#state.index)) of
        true  ->
            NodeClock = swc_node:add(State#state.clock, {PeerId, 0}),
            MyPeersIds = ordsets:add_element(PeerId, State#state.peers_ids),
            WM = case ordsets:size(MyPeersIds) == (?REPLICATION_FACTOR-1)*2 of
                true -> % we have all the peers Ids, now broadcast that list to our peers
                    % lager:info("Peers Ids DONE!"),
                    CurrentPeers = dotted_db_utils:peers(State#state.index),
                    broadcast_my_peers_to_my_peers(CurrentPeers, State#state.id, MyPeersIds),
                    swc_watermark:add_peer(State#state.watermark, State#state.id, MyPeersIds);
                false ->
                    % lager:info("Getting Peers Ids ~p/~p",[ordsets:size(MyPeersIds), (?REPLICATION_FACTOR-1)*2]),
                    State#state.watermark
            end,
            {ok, State#state{clock=NodeClock, watermark=WM, peers_ids=MyPeersIds}};
        false ->
            lager:info("WRONG NODE ID! IxNd: ~p ", [IndexNode]),
            {ok, State}
    end;
%% Report Tick
handle_info(report_tick, State=#state{stats=false}) ->
    schedule_report(State#state.report_interval),
    {ok, State};
handle_info(report_tick, State=#state{stats=true}) ->
    {_, NextState} = report(State),
    schedule_report(State#state.report_interval),
    {ok, NextState};
%% Buffer Strip Tick
handle_info(strip_keys, State=#state{mode=recovering}) ->
    % schedule the strip for keys that still have causal context at the moment
    lager:warning("Not stripping keys because we are in recovery mode."),
    schedule_strip_keys(State#state.buffer_strip_interval),
    {ok, State};
handle_info(strip_keys, State=#state{mode=normal, non_stripped_keys=NSKeys}) ->
    NSKeys2 = read_strip_write(NSKeys, State),
    % Optionally collect stats
    case State#state.stats of
        true ->
            % {D1,W1} = NSKeys,
            % {D2,W2} = NSKeys2,
            % NumNSKeys = lists:sum([dict:size(Dict) || {_,Dict} <- W1]) + length(D1),
            % NumNSKeys2 = lists:sum([dict:size(Dict) || {_,Dict} <- W2]) + length(D2),
            % CCF = NumNSKeys * ?REPLICATION_FACTOR,
            % CCS = NumNSKeys2 * ?REPLICATION_FACTOR, % we don't really know, but assume the worst
            % EntryExampleSize = byte_size(term_to_binary({State#state.id, 123345})),
            % MetaF = EntryExampleSize * ?REPLICATION_FACTOR * NumNSKeys,
            % MetaS = EntryExampleSize * CCS,
            % dotted_db_stats:update_key_meta(State#state.index, NumNSKeys, MetaF, MetaS, CCF, CCS),
            ok;
        false -> ok
    end,
    % schedule the strip for keys that still have causal context at the moment
    schedule_strip_keys(State#state.buffer_strip_interval),
    {ok, State#state{non_stripped_keys=NSKeys2}};
handle_info(Info, State) ->
    lager:info("unhandled_info: ~p",[Info]),
    {ok, State}.

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
        element(1, Cmd) == sync_missing orelse
        element(1, Cmd) == sync_repair ->
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
handle_handoff_command(Cmd={write, {ReqID, _, Key, _, _, _FSMTime}}, Sender, State) ->
    lager:info("HAND_WRITE: {~p, ~p} // Key: ~p",[State#state.id, node(), Key]),
    % do the local coordinating write
    {reply, {ok, ReqID, NewObject}, State2} = handle_command(Cmd, Sender, State),
    % send the ack to the PUT_FSM
    riak_core_vnode:reply(Sender, {ok, ReqID, NewObject}),
    % create a new request to forward the replication of this new object
    NewCommand = {replicate, {ReqID, Key, NewObject, ?DEFAULT_NO_REPLY}},
    {forward, NewCommand, State2};

%% Handle all other commands locally (only gets?)
handle_handoff_command(Cmd, Sender, State) ->
    lager:info("Handoff command ~p at ~p", [Cmd, State#state.id]),
    handle_command(Cmd, Sender, State).

handoff_starting(TargetNode, State) ->
    lager:info("HAND_START: {~p, ~p} to ~p",[State#state.index, node(), TargetNode]),
    %% save the vnode state, if not empty
    ok = case State#state.clock =:= swc_node:new() of
        true -> ok;
        false ->
            Key = {?DEFAULT_BUCKET, {?VNODE_STATE_KEY, State#state.index}},
            NodeState = {State#state.clock, State#state.dotkeymap, State#state.watermark, State#state.non_stripped_keys},
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
            {NodeKey, {NodeClock, DotKeyMap, Watermark, NSK}} ->
                NodeClock2 = swc_node:join(NodeClock, State#state.clock),
                State#state{clock = NodeClock2, dotkeymap = DotKeyMap, watermark = Watermark, non_stripped_keys = NSK};
            {Key, Obj} ->
                {noreply, State2} = handle_command({replicate, {dummy_req_id, Key, Obj, ?DEFAULT_NO_REPLY}}, undefined, State),
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
            lager:info("IxNd:~p // Clock:~p // DKM:~p // Watermark:~p",
                [{State#state.index, node()}, State#state.clock, State#state.dotkeymap, State#state.watermark] ),
            lager:info("GOOD_DROP: {~p, ~p}",[State#state.index, node()]);
        false -> ok
    end,
    true = delete_ets_all_keys(State),
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_read({read, ReqID, Key}, State) ->
    Response =
        case dotted_db_storage:get(State#state.storage, Key) of
            {error, not_found} ->
                % there is no key K in this node
                % create an empty "object" and fill its causality with the node clock
                % this is needed to ensure that deletes "win" over old writes at the coordinator
                {ok, dotted_db_object:fill(Key, State#state.clock, dotted_db_object:new())};
            {error, Error} ->
                % some unexpected error
                lager:error("Error reading a key from storage (command read): ~p", [Error]),
                % return the error
                {error, Error};
            Obj ->
                % get and fill the causal history of the local object
                {ok, dotted_db_object:fill(Key, State#state.clock, Obj)}
        end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    IndexNode = {State#state.index, node()},
    {reply, {ok, ReqID, IndexNode, Response}, State}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_write({write, {ReqID, Operation, Key, Value, Context, FSMTime}}, State) ->
    Now = undefined,% os:timestamp(),
    % get and fill the causal history of the local key
    DiskObject = guaranteed_get(Key, State),
    % discard obsolete values w.r.t the causal context
    DiscardObject = dotted_db_object:discard_values(Context, DiskObject),
    % generate a new dot for this write/delete and add it to the node clock
    {Dot, NodeClock} = swc_node:event(State#state.clock, State#state.id),
    % test if this is a delete; if not, add dot-value to the object container
    NewObject0 =
        case Operation of
            ?DELETE_OP  -> % DELETE
                dotted_db_object:add_value({State#state.id, Dot}, ?DELETE_OP, DiscardObject);
            ?WRITE_OP   -> % PUT
                dotted_db_object:add_value({State#state.id, Dot}, Value, DiscardObject)
        end,
    NewObject = dotted_db_object:set_fsm_time(FSMTime, NewObject0),
    % save the new k\v and remove unnecessary causal information
    _= strip_save_batch([{Key, NewObject}], State#state{clock=NodeClock}, Now),
    % append the key to the tail of the KDM
    DotKeyMap = swc_dotkeymap:add_key(State#state.dotkeymap, State#state.id, Key, Dot),
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID, NewObject}, State#state{clock = NodeClock, dotkeymap = DotKeyMap}}.


handle_replicate({replicate, {ReqID, Key, NewObject, NoReply}}, State) ->
    Now = undefined,% os:timestamp(),
    NodeClock = dotted_db_object:add_to_node_clock(State#state.clock, NewObject),
    % append the key to the KDM
    DotKeyMap = swc_dotkeymap:add_objects(State#state.dotkeymap, [{Key, dotted_db_object:get_container(NewObject)}]),
    % get and fill the causal history of the local key
    DiskObject = guaranteed_get(Key, State),
    % synchronize both objects
    FinalObject = dotted_db_object:sync(NewObject, DiskObject),
    % test if the FinalObject has newer information
    NSK = case dotted_db_object:equal(FinalObject, DiskObject) of
        true ->
            lager:debug("Replicated object is ignored (already seen)"),
            State#state.non_stripped_keys;
        false ->
            % save the new object, while stripping the unnecessary causality
            case strip_save_batch([{Key, FinalObject}], State#state{clock=NodeClock, dotkeymap=DotKeyMap}, Now) of
                [] -> State#state.non_stripped_keys;
                _  -> add_key_to_NSK(Key, NewObject, State#state.non_stripped_keys)
            end
    end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    NewState = State#state{clock = NodeClock, dotkeymap = DotKeyMap, non_stripped_keys = NSK},
    % return the updated node state
    case NoReply of
        true  -> {noreply, NewState};
        false -> {reply, {ok, ReqID}, NewState}
    end.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_sync_start({sync_start, ReqID}, State=#state{mode=recovering}) ->
    {reply, {cancel, ReqID, recovering}, State};
handle_sync_start({sync_start, ReqID}, State=#state{mode=normal}) ->
    % choose a peer at random
    NodeB = dotted_db_utils:random_from_list(dotted_db_utils:peers(State#state.index)),
    % get my peers
    PeersIDs = swc_watermark:peers(State#state.watermark),
    % send a sync message to that node
    {reply, {ok, ReqID, State#state.id, NodeB, State#state.clock, PeersIDs}, State}.


handle_sync_missing({sync_missing, ReqID, _, _, _}, _Sender, State=#state{mode=recovering}) ->
    {reply, {cancel, ReqID, recovering}, State};
handle_sync_missing({sync_missing, ReqID, _RemoteID={RemoteIndex,_}, RemoteClock, RemotePeers}, Sender, State=#state{mode=normal}) ->
    spawn(fun() ->
        % calculate what dots are present locally that the asking node does not have
        MissingDots = swc_node:missing_dots(State#state.clock, RemoteClock, RemotePeers),
        % get the keys corresponding to the missing dots,
        {MissingKeys0, _DotsNotFound} = swc_dotkeymap:get_keys(State#state.dotkeymap, MissingDots),
        % remove duplicate keys
        MissingKeys = sets:to_list(sets:from_list(MissingKeys0)),
        % filter the keys that the asking node does not replicate
        RelevantMissingKeys = filter_irrelevant_keys(MissingKeys, RemoteIndex),
        % get each key's respective Object and strip any unnecessary causal information to save network
        StrippedObjects = guaranteed_get_strip_list(RelevantMissingKeys, State),
        % DotsNotFound2 = [ {A,B} || {A,B} <- DotsNotFound, B =/= [] andalso A =:= State#state.id],
        % case DotsNotFound2 of
        %     [] -> ok;
        %     _  -> lager:info("\n\n~p to ~p:\n\tNotFound: ~p \n\tMiKeys: ~p \n\tRelKey: ~p \n\tKDM: ~p \n\tStrip: ~p \n\tBVV: ~p \n",
        %             [State#state.id, RemoteID, DotsNotFound2, MissingKeys, RelevantMissingKeys, State#state.dotkeymap, StrippedObjects, State#state.clock])
        % end,
        % Optionally collect stats
        case ?STAT_SYNC andalso State#state.stats andalso MissingKeys > 0 andalso length(StrippedObjects) > 0 of
            true ->
                Ratio_Relevant_Keys = round(100*length(RelevantMissingKeys)/max(1,length(MissingKeys))),
                SRR = {histogram, sync_relevant_ratio, Ratio_Relevant_Keys},

                Ctx_Sent_Strip = [dotted_db_object:get_context(Obj) || {_Key, Obj} <- StrippedObjects],
                Sum_Ctx_Sent_Strip = lists:sum([length(VV) || VV <- Ctx_Sent_Strip]),
                Ratio_Sent_Strip = Sum_Ctx_Sent_Strip/max(1,length(StrippedObjects)),
                SSDS = {histogram, sync_sent_dcc_strip, Ratio_Sent_Strip},

                Size_Meta_Sent = byte_size(term_to_binary(Ctx_Sent_Strip)),
                SCS = {histogram, sync_context_size, Size_Meta_Sent},
                SMS = {histogram, sync_metadata_size, byte_size(term_to_binary(RemoteClock))},

                Payload_Sent_Strip = [{Key, dotted_db_object:get_values(Obj)} || {Key, Obj} <- StrippedObjects],
                Size_Payload_Sent = byte_size(term_to_binary(Payload_Sent_Strip)),
                SPS = {histogram, sync_payload_size, Size_Payload_Sent},

                dotted_db_stats:notify2([SRR, SSDS, SCS, SMS, SPS]),
                ok;
            false -> ok
        end,
        % send the final objects and the base (contiguous) dots of the node clock to the asking node
        riak_core_vnode:reply(
            Sender,
            {   ok,
                ReqID,
                State#state.id,
                State#state.clock,
                State#state.watermark,
                swc_watermark:peers(State#state.watermark),
                StrippedObjects
            })
    end),
    {noreply, State}.

handle_sync_repair({sync_repair, {ReqID, _, _, _, _, NoReply}}, State=#state{mode=recovering}) ->
    lager:warning("repairing stuff"),
    case NoReply of
        true  -> {noreply, State};
        false -> {reply, {cancel, ReqID, recovering}, State}
    end;
handle_sync_repair({sync_repair, {ReqID, _RemoteNodeID={RemoteIndex,_}, RemoteClock, RemoteWatermark, MissingObjects, NoReply}},
                   State=#state{mode=normal, index=MyIndex, clock=LocalClock, dotkeymap=DotKeyMap, watermark=Watermark1}) ->
    Now = os:timestamp(),
    % add information about the remote clock to our clock, but only for the remote node entry
    LocalClock2 = sync_clocks(LocalClock, RemoteClock, RemoteIndex),
    % get the local objects corresponding to the received objects and fill the causal history for all of them
    FilledObjects =
        [{ Key, dotted_db_object:fill(Key, RemoteClock, Obj), guaranteed_get(Key, State) }
         || {Key,Obj} <- MissingObjects],
    % synchronize / merge the remote and local objects
    SyncedObjects = [{ Key, dotted_db_object:sync(Remote, Local), Local } || {Key, Remote, Local} <- FilledObjects],
    % filter the objects that are not missing after all
    RealMissingObjects = [{ Key, Synced } || {Key, Synced, Local} <- SyncedObjects,
                                             (not dotted_db_object:equal_values(Synced,Local)) orelse
                                             (dotted_db_object:get_values(Synced)==[] andalso
                                              dotted_db_object:get_values(Local)==[])],
    % add each new dot to our node clock
    NodeClock = lists:foldl(fun ({_K,O}, Acc) -> dotted_db_object:add_to_node_clock(Acc, O) end, LocalClock2, RealMissingObjects),
    % add new keys to the Dot-Key Mapping
    DKM = swc_dotkeymap:add_objects(DotKeyMap,
            lists:map(fun ({Key,Obj}) -> {Key, dotted_db_object:get_container(Obj)} end, RealMissingObjects)),
    % save the synced objects and strip their causal history
    NonStrippedObjects = strip_save_batch(RealMissingObjects, State#state{clock=NodeClock}, Now),
    % schedule a later strip attempt for non-stripped synced keys
    NSK = add_keys_to_NSK(NonStrippedObjects, State#state.non_stripped_keys),
    % update my watermark
    Watermark3 = update_watermark_after_sync(Watermark1, RemoteWatermark, MyIndex, RemoteIndex, NodeClock, RemoteClock),
    % Garbage Collect keys from the dotkeymap and delete keys with no causal context
    update_jump_clock(RemoteIndex),
    State2 = gc_dotkeymap(State#state{clock=NodeClock, dotkeymap=DKM, non_stripped_keys=NSK, watermark=Watermark3}),
    % Optionally collect stats
    case ?STAT_SYNC andalso State2#state.stats of
        true ->
            Repaired = length(RealMissingObjects),
            Sent = length(MissingObjects),
            Hit_Ratio = 100*Repaired/max(1, Sent),
            SL = case Sent =/= 0 of
                true ->
                    [{histogram, sync_hit_ratio, round(Hit_Ratio)},
                    {histogram, sync_sent_missing, Sent},
                    {histogram, sync_sent_truly_missing, Repaired}];
                false ->
                    [{histogram, sync_hit_ratio, 100}]
            end,
            dotted_db_stats:notify2([{histogram, sync_metadata_size, byte_size(term_to_binary(RemoteClock))} | SL]),
            ok;
        false ->
            ok
    end,
    % return the updated node state
    case NoReply of
        true  -> {noreply, State2};
        false -> {reply, {ok, ReqID}, State2}
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Restarting Vnode (and recovery of keys)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% On the restarting node
handle_restart({restart, ReqID}, State=#state{mode=recovering}) ->
    {reply, {cancel, ReqID, recovering}, State};
handle_restart({restart, ReqID}, State=#state{mode=normal}) ->
    OldVnodeID = State#state.id,
    NewVnodeID = new_vnode_id(State#state.index),
    % keep track from now on on which peers this node did a sync, to be able to
    % jump the old vnode id in the node clock when we synced with every other peer
    add_removed_vnode_jump_clock(OldVnodeID),
    % changes all occurences of the old id for the new id,
    % while at the same time resetting the column and the line of the new id,
    % which means that this node does not know about anything at the moment,
    % nor other vnodes know about this new id updates (since there are none at the moment).
    NewWatermark0 = swc_watermark:retire_peer(State#state.watermark, OldVnodeID, NewVnodeID),
    % reset the entire watermark
    NewWatermark = swc_watermark:reset_counters(NewWatermark0),
    CurrentPeers = dotted_db_utils:peers(State#state.index),
    lager:info("RESTART:\nOLD: ~p\nNEW: ~p\nPEERS: ~p",[OldVnodeID, NewVnodeID, CurrentPeers]),
    true = delete_ets_all_keys(State),
    NewAtomID = create_ets_all_keys(NewVnodeID),
    {ok, Storage1} = dotted_db_storage:drop(State#state.storage),
    ok = dotted_db_storage:close(Storage1),
    % open the storage backend for the key-values of this vnode
    {_, NewStorage} = open_storage(State#state.index),
    ok = save_vnode_state(State#state.dets, {NewVnodeID, swc_node:new(), swc_dotkeymap:new(), NewWatermark, []}),
    % store the number of full-syncs
    put(nr_full_syncs, 0),
    {reply, {ok, ReqID, {ReqID, State#state.index, OldVnodeID, NewVnodeID}, CurrentPeers},
        State#state{
            id                  = NewVnodeID,
            atom_id             = NewAtomID,
            clock               = swc_node:new(),
            dotkeymap           = swc_dotkeymap:new(),
            watermark           = NewWatermark,
            non_stripped_keys   = {[],[]},
            recover_keys        = [],
            storage             = NewStorage,
            syncs               = initialize_syncs(State#state.index),
            updates_mem         = 0,
            mode                = recovering}}.

%% On the good nodes
handle_inform_peers_restart({inform_peers_restart, {ReqID, RestartingVnodeIndex, OldVnodeID, NewVnodeID}}, State) ->
    % keep track from now on on which peers this node did a sync, to be able to
    % jump the old vnode id in the node clock when we synced with every other peer
    add_removed_vnode_jump_clock(OldVnodeID),
    % replace the old peer entry in the watermarks of this vnode's peers also
    CurrentPeers = dotted_db_utils:peers(State#state.index),
    replace_peer(CurrentPeers, OldVnodeID, NewVnodeID),
    % update the "mypeers" set
    MyPeersIds = ordsets:add_element(NewVnodeID, ordsets:del_element(OldVnodeID, State#state.peers_ids)),
    % add the new node id to the node clock
    NewClock = swc_node:add(State#state.clock, {NewVnodeID, 0}),
    % replace the old entry for the new entry in the watermark
    NewWatermark = swc_watermark:retire_peer(State#state.watermark, OldVnodeID, NewVnodeID),
    % add the new node id to the node clock
    {AllKeys,_} = ets_get_all_keys(State),
    % filter irrelevant keys from the perspective of the restarting vnode
    RelevantKeys = filter_irrelevant_keys(AllKeys, RestartingVnodeIndex),
    {Now, Later} = lists:split(min(?MAX_KEYS_SENT_RECOVERING,length(RelevantKeys)), RelevantKeys),
    lager:info("Restart transfer => Now: ~p Later: ~p",[length(Now), length(Later)]),
    % get each key's respective Object and strip any unnecessary causal information to save network bandwidth
    StrippedObjects = guaranteed_get_strip_list(Now, State#state{clock=NewClock}),
    % save the rest of the keys for later (if there's any)
    {LastBatch, RecoverKeys} = case Later of
        [] -> {true, State#state.recover_keys};
        _ -> {false, [{NewVnodeID, Later} | State#state.recover_keys]}
    end,
    {reply, { ok, stage1, ReqID, {
                ReqID,
                {State#state.index, node()},
                OldVnodeID,
                NewClock,
                StrippedObjects,
                NewWatermark,
                LastBatch % is this the last batch?
            }}, State#state{clock=NewClock, peers_ids=MyPeersIds, watermark=NewWatermark, recover_keys=RecoverKeys}}.

%% On the restarting node
handle_recover_keys({recover_keys, {ReqID, RemoteVnode, _OldVnodeID={_,_}, RemoteClock, Objects, _RemoteWatermark, _LastBatch=false}}, State) ->
    % save the objects and return the ones that were not totally filtered
    {NodeClock, DKM, NonStrippedObjects} = fill_strip_save_kvs(Objects, RemoteClock, State#state.clock, State, os:timestamp()),
    % % add new keys to the Dot-Key Mapping
    % DKM = swc_dotkeymap:add_objects(State#state.dotkeymap,
    %         lists:map(fun ({Key,Obj}) -> {Key, dotted_db_object:get_container(Obj)} end, Objects)),
    % schedule a later strip attempt for non-stripped synced keys
    NSK = add_keys_to_NSK(NonStrippedObjects, State#state.non_stripped_keys),
    {reply, {ok, stage2, ReqID, RemoteVnode}, State#state{clock=NodeClock, dotkeymap=DKM, non_stripped_keys=NSK}};
%% On the restarting node
handle_recover_keys({recover_keys, {ReqID, RemoteVnode={RemoteIndex,_}, _OldID, RemoteClock, Objects, RemoteWatermark, _LastBatch=true}}, State) ->
    NodeClock0 = sync_clocks(State#state.clock, RemoteClock, RemoteIndex),
    % save the objects and return the ones that were not totally filtered
    {NodeClock, DKM, NonStrippedObjects} = fill_strip_save_kvs(Objects, RemoteClock, State#state.clock, State#state{clock=NodeClock0}, os:timestamp()),
    % schedule a later strip attempt for non-stripped synced keys
    NSK = add_keys_to_NSK(NonStrippedObjects, State#state.non_stripped_keys),
    % update my watermark
    Watermark = update_watermark_after_sync(State#state.watermark, RemoteWatermark, State#state.index, RemoteIndex, NodeClock, RemoteClock),
    {Mode, NodeClock3} = case get(nr_full_syncs) of
        undefined ->
            {normal, NodeClock};
        N when N >= (?REPLICATION_FACTOR-1)*2-1 ->
            erase(nr_full_syncs),
            % jump the base counter of all old ids in the node clock, to make sure we "win"
            % against all keys potentially not stripped yet because of that old id
            NodeClock2 = jump_node_clock_by_index(NodeClock, State#state.id, State#state.index, 20000),
            % NodeClock2 = swc_node:store_entry(OldVnodeID, {Base+10000,0}, NodeClock),
            {normal, NodeClock2};
        N when N < (?REPLICATION_FACTOR-1)*2-1 ->
            put(nr_full_syncs, N+1),
            {recovering, NodeClock}
    end,
    {reply, {ok, stage4, ReqID, RemoteVnode}, State#state{clock=NodeClock3, dotkeymap=DKM, non_stripped_keys=NSK, watermark=Watermark, mode=Mode}}.

%% On the good nodes
handle_inform_peers_restart2({inform_peers_restart2, {ReqID, NewVnodeID, OldVnodeID}}, State) ->
    {LastBatch1, Objects, RecoverKeys1} =
        case proplists:get_value(NewVnodeID, State#state.recover_keys) of
            undefined ->
                {true, [], State#state.recover_keys};
            RelevantKeys ->
                RK = proplists:delete(NewVnodeID, State#state.recover_keys),
                {Now, Later} = lists:split(min(?MAX_KEYS_SENT_RECOVERING,length(RelevantKeys)), RelevantKeys),
                % get each key's respective Object and strip any unnecessary causal information to save network bandwidth
                StrippedObjects = guaranteed_get_strip_list(Now, State),
                % save the rest of the keys for later (if there's any)
                {LastBatch, RecoverKeys} = case Later of
                    [] -> {true, RK};
                    _ -> {false, [{NewVnodeID, Later} | RK]}
                end,
                {LastBatch, StrippedObjects, RecoverKeys}
        end,
    {reply, { ok, stage3, ReqID, {
                ReqID,
                {State#state.index, node()},
                OldVnodeID,
                State#state.clock,
                Objects,
                State#state.watermark,
                LastBatch1 % is this the last batch?
            }}, State#state{recover_keys=RecoverKeys1}}.





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Aux functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the Object associated with the Key.
% By default, we want to return a filled causality, unless we get a storage error.
% If the key does not exists or for some reason, the storage returns an
% error, return an empty Object (also filled).
guaranteed_get(Key, State) ->
    case dotted_db_storage:get(State#state.storage, Key) of
        {error, not_found} ->
            % there is no key K in this node
            Obj = dotted_db_object:new(),
            Obj2 = dotted_db_object:set_fsm_time(ets_get_fsm_time(State#state.atom_id, Key), Obj),
            dotted_db_object:fill(Key, State#state.clock, Obj2);
        {error, Error} ->
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            dotted_db_object:new();
        Obj ->
            % get and fill the causal history of the local object
            dotted_db_object:fill(Key, State#state.clock, Obj)
    end.

guaranteed_get_strip_list(Keys, State) ->
    lists:map(fun(Key) -> guaranteed_get_strip(Key, State) end, Keys).

guaranteed_get_strip(Key, State) ->
    case dotted_db_storage:get(State#state.storage, Key) of
        {error, not_found} ->
            % there is no key K in this node
            {Key, dotted_db_object:set_fsm_time(ets_get_fsm_time(State#state.atom_id, Key),
                    dotted_db_object:new())};
        {error, Error} ->
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET) (2): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            {Key, dotted_db_object:set_fsm_time(ets_get_fsm_time(State#state.atom_id, Key),
                    dotted_db_object:new())};
        Obj ->
            % get and fill the causal history of the local object
            {Key, dotted_db_object:strip(State#state.clock, Obj)}
    end.

filter_irrelevant_keys(Keys, Index) ->
    FunFilterIrrelevant = fun(Key) -> lists:member(Index, dotted_db_utils:replica_nodes_indices(Key)) end,
    lists:filter(FunFilterIrrelevant, Keys).

% @doc Saves the relevant vnode state to the storage.
save_vnode_state(Dets, State={Id={Index,_},_,_,_,_}) ->
    Key = {?VNODE_STATE_KEY, Index},
    ok = dets:insert(Dets, {Key, State}),
    ok = dets:sync(Dets),
    lager:debug("Saved state for vnode ~p.",[Id]),
    ok.

% @doc Reads the relevant vnode state from the storage.
read_vnode_state(Index) ->
    Folder = "data/vnode_state/",
    ok = filelib:ensure_dir(Folder),
    FileName = filename:join(Folder, integer_to_list(Index)),
    Ref = list_to_atom(integer_to_list(Index)),
    {ok, Dets} = dets:open_file(Ref,[{type, set},
                                    {file, FileName},
                                    {auto_save, infinity},
                                    {min_no_slots, 1}]),
    Key = {?VNODE_STATE_KEY, Index},
    case dets:lookup(Dets, Key) of
        [] -> % there isn't a past vnode state stored
            {Dets, not_found};
        {error, Error} -> % some unexpected error
            {Dets, error, Error};
        [{Key, State={{Index,_},_,_,_,_}}] ->
            {Dets, State}
    end.

% @doc Initializes the "watermark" matrix with 0's for peers of this vnode.
initialize_watermark(_NodeId={Index,_}) ->
    lager:debug("Initialize watermark @ IndexNode: ~p",[{Index,node()}]),
    % get the Index and Node of this node's peers, i.e., all nodes that replicates any subset of local keys.
    IndexNodes = [ IndexNode || IndexNode <- dotted_db_utils:peers(Index)],
    % for replication factor N = 3, the numbers of peers should be 4 (2 vnodes before and 2 after).
    (?REPLICATION_FACTOR-1)*2 = length(IndexNodes),
    % ask each vnode for their current vnode ID
    get_vnode_id(IndexNodes),
    ok.

% @doc Initializes the "sync" stats for peers of this vnode.
initialize_syncs(_Index) ->
    [{dummy_node_id,0,0,0,0}].
%     % get this node's peers, i.e., all nodes that replicates any subset of local keys.
%     PeerIDs = [ ID || {ID, _Node} <- dotted_db_utils:peers(Index)],
%     % for replication factor N = 3, the numbers of peers should be 4 (2 vnodes before and 2 after).
%     (?REPLICATION_FACTOR-1)*2 = length(PeerIDs),
%     Now = os:timestamp(),
%     Syncs = lists:foldl(fun (ID, List) -> [{ID,0,0,Now,Now} | List] end , [], PeerIDs),
%     (?REPLICATION_FACTOR-1)*2 = length(Syncs),
%     Syncs.


% @doc Returns the Storage for this vnode.
open_storage(Index) ->
    % get the preferred backend in the configuration file, defaulting to ETS if
    % there is no preference.
    {Backend, Options} = case application:get_env(dotted_db, storage_backend, ets) of
        leveldb   -> {{backend, leveldb}, []};
        ets       -> {{backend, ets}, []};
        bitcask   -> {{backend, bitcask}, [{db_opts,[
                read_write,
                {sync_strategy, application:get_env(dotted_db, bitcask_io_sync, none)},
                {io_mode, application:get_env(dotted_db, bitcask_io_mode, erlang)},
                {merge_window, application:get_env(dotted_db, bitcask_merge_window, never)}]}]}
    end,
    lager:debug("Using ~p for vnode ~p.",[Backend,Index]),
    % give the name to the backend for this vnode using its position in the ring.
    DBName = filename:join("data/objects/", integer_to_list(Index)),
    {ok, Storage} = dotted_db_storage:open(DBName, Backend, Options),
    {Backend, Storage}.

% @doc Close the key-value backend, save the vnode state and close the DETS file.
close_all(undefined) -> ok;
close_all(State=#state{ id          = Id,
                        storage     = Storage,
                        clock       = NodeClock,
                        watermark   = Watermark,
                        dotkeymap   = DotKeyMap,
                        non_stripped_keys = NSK,
                        dets        = Dets } ) ->
    case dotted_db_storage:close(Storage) of
        ok -> ok;
        {error, Reason} ->
            lager:warning("Error on closing storage: ~p",[Reason])
    end,
    ok = save_vnode_state(Dets, {Id, NodeClock, DotKeyMap, Watermark, NSK}),
    true = delete_ets_all_keys(State),
    ok = dets:close(Dets).


gc_dotkeymap(State=#state{dotkeymap = DotKeyMap, watermark = Watermark, non_stripped_keys = NSK}) ->
    case is_watermark_up_to_date(Watermark) of
        true ->
            % remove the keys from the dotkeymap that have a dot (corresponding to their position) smaller than the
            % minimum dot, i.e., this update is known by all nodes that replicate it and therefore can be removed
            % from the dotkeymap;
            {DotKeyMap2, RemovedKeys} = swc_dotkeymap:prune(DotKeyMap, Watermark),
            % remove entries in watermark from retired peers, that aren't needed anymore
            % (i.e. there isn't keys coordinated by those retired nodes in the DotKeyMap)
            OldPeersStillNotSynced = get_old_peers_still_not_synced(),
            Watermark2 = swc_watermark:prune_retired_peers(Watermark, DotKeyMap2, OldPeersStillNotSynced),
            % add the non stripped keys to the node state for later strip attempt
            NSK2 = add_keys_from_dotkeymap_to_NSK(RemovedKeys, NSK),
            State#state{dotkeymap = DotKeyMap2, non_stripped_keys=NSK2, watermark=Watermark2};
        false ->
            {WM,_} = State#state.watermark,
            lager:info("Watermark not up to date: ~p entries, mode: ~p",[orddict:size(WM), State#state.mode]),
            [case orddict:size(V) =:= (?REPLICATION_FACTOR*2)-1 of
                 true -> lager:info("\t ~p for ~p \n", [orddict:size(V), K]);
                 false -> lager:info("\t ~p for ~p \n\t\t ~p\n", [orddict:size(V), K, V])
             end || {K,V} <- WM],
            swc_dotkeymap:empty(DotKeyMap) andalso initialize_watermark(State#state.id),
            State
    end.


-spec schedule_strip_keys(non_neg_integer()) -> ok.
schedule_strip_keys(Interval) ->
    erlang:send_after(Interval, self(), strip_keys),
    ok.

-spec schedule_report(non_neg_integer()) -> ok.
schedule_report(Interval) ->
    %% Perform tick every X seconds
    erlang:send_after(Interval, self(), report_tick),
    ok.

-spec report(state()) -> {any(), state()}.
report(State=#state{    id                  = Id,
                        clock               = NodeClock,
                        watermark           = Watermark,
                        dotkeymap           = DotKeyMap,
                        non_stripped_keys   = NSK,
                        dets                = Dets,
                        updates_mem         = UpMem } ) ->
    report_stats(State),
    % increment the updates since saving
    UpdatesMemory =  case UpMem =< ?UPDATE_LIMITE*50 of
        true -> % it's still early to save to storage
            UpMem + 1;
        false ->
            % it's time to persist vnode state
            save_vnode_state(Dets, {Id, NodeClock, DotKeyMap, Watermark, NSK}),
            % restart the counter
            0
    end,
    {ok, State#state{updates_mem=UpdatesMemory}}.


report_stats(State=#state{stats=true}) ->
    case (not swc_dotkeymap:empty(State#state.dotkeymap)) andalso
         State#state.clock =/= swc_node:new() andalso
         State#state.watermark =/= swc_watermark:new() of
        true ->
            SSL = case ?STAT_STATE_LENGTH of
                false -> [];
                true ->
                    KLLEN = {histogram, kl_len, swc_dotkeymap:size(State#state.dotkeymap)},
                    MissingDots = [ miss_dots(Entry) || {_,Entry} <- State#state.clock ],
                    BVVMD = {histogram, bvv_missing_dots, average(MissingDots)},
                    {Del,Wrt} = State#state.non_stripped_keys,
                    NumNSKeys = lists:sum([dict:size(Map) || {_, Map} <- Wrt]) + length(Del),
                    NSKN = {histogram, nsk_number, NumNSKeys},
                    [KLLEN, BVVMD, NSKN]
            end,

            SSS = case ?STAT_STATE_SIZE of
                false -> [];
                true ->
                    KLSIZE = {histogram, kl_size, size(term_to_binary(State#state.dotkeymap))},
                    BVVSIZE = {histogram, bvv_size, size(term_to_binary(State#state.clock))},
                    NSKSIZE = {histogram, nsk_size, size(term_to_binary(State#state.non_stripped_keys))},
                    [KLSIZE, BVVSIZE, NSKSIZE]
            end,

            SD = case ?STAT_DELETES of
                false -> [];
                true ->
                    ADelKeys = length(ets_get_actual_deleted(State#state.atom_id)),
                    IDelKeys = length(ets_get_issued_deleted(State#state.atom_id)),
                    DI = {histogram, deletes_incomplete, IDelKeys},
                    DC = {histogram, deletes_completed, ADelKeys},

                    IWKeys = length(ets_get_issued_written(State#state.atom_id)),
                    FWKeys = length(ets_get_final_written(State#state.atom_id)),
                    WI = {histogram, write_incomplete, IWKeys},
                    WC = {histogram, write_completed, FWKeys},
                    [DI,DC,WI,WC]
            end,

            dotted_db_stats:notify2(SD ++ SSS ++ SSL),
            ok;
        false ->
            ok
    end,
    {ok, State}.

miss_dots({N,B}) ->
    case values_aux(N,B,[]) of
        [] -> 0;
        L  -> lists:max(L) - N - length(L)
    end.
values_aux(_,0,L) -> L;
values_aux(N,B,L) ->
    M = N + 1,
    case B rem 2 of
        0 -> values_aux(M, B bsr 1, L);
        1 -> values_aux(M, B bsr 1, [ M | L ])
    end.

average(L) ->
    lists:sum(L) / max(1,length(L)).



strip_save_batch(O,S,Now) -> strip_save_batch(O,S,Now,true).
strip_save_batch(Objects, State, Now, ETS) ->
    strip_save_batch(Objects, State, Now, {[],[]}, ETS).

strip_save_batch([], State, _Now, {NSK, StrippedObjects}, _ETS) ->
    ok = dotted_db_storage:write_batch(State#state.storage, StrippedObjects),
    NSK;
strip_save_batch([{Key, Obj} | Objects], S=#state{atom_id=ID}, Now, {NSK, StrippedObjects}, ETS) ->
    % removed unnecessary causality from the Object, based on the current node clock
    StrippedObj = dotted_db_object:strip(S#state.clock, Obj),
    {Values, Context} = dotted_db_object:get_container(StrippedObj),
    Values2 = [{D,V} || {D,V} <- Values, V =/= ?DELETE_OP],
    StrippedObj2 = dotted_db_object:set_container({Values2, Context}, StrippedObj),
    % the resulting object is one of the following options:
    %  0 * it has no value but has causal history -> it's a delete, but still must be persisted
    %  1 * it has no value and no causal history -> can be deleted
    %  2 * has values, with causal context -> it's a normal write and we should persist
    %  3 * has values, but no causal context -> it's the final form for this write
    Acc = case {Values2, Context} of
        {[],[]} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 0),
            ETS andalso ets_set_status(ID, Key, ?ETS_DELETE_STRIP),
            ETS andalso ets_set_strip_time(ID, Key, Now),
            ETS andalso notify_strip_delete_latency(Now, Now),
            ETS andalso ets_set_dots(ID, Key, []),
            {NSK, [{delete, Key}|StrippedObjects]};
        {_ ,[]} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 1),
            ETS andalso ets_set_status(ID, Key, ?ETS_WRITE_STRIP),
            ETS andalso ets_set_strip_time(ID, Key, Now),
            ETS andalso notify_strip_write_latency(Now, Now),
            ETS andalso ets_set_dots(ID, Key, get_value_dots_for_ets(StrippedObj)),
            {NSK, [{put, Key, StrippedObj2}|StrippedObjects]};
        {[],_CC} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, length(Context)),
            ETS andalso ets_set_status(ID, Key, ?ETS_DELETE_NO_STRIP),
            ETS andalso ets_set_dots(ID, Key, get_value_dots_for_ets(StrippedObj)),
            {[{Key, StrippedObj2}|NSK], [{put, Key, StrippedObj2}|StrippedObjects]};
        {_ ,_CC} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, length(Context)+1),
            ETS andalso ets_set_status(ID, Key, ?ETS_WRITE_NO_STRIP),
            ETS andalso ets_set_dots(ID, Key, get_value_dots_for_ets(StrippedObj)),
            {[{Key, StrippedObj2}|NSK], [{put, Key, StrippedObj2}|StrippedObjects]}
    end,
    ETS andalso notify_write_latency(dotted_db_object:get_fsm_time(StrippedObj), Now),
    ETS andalso ets_set_write_time(ID, Key, Now),
    ETS andalso ets_set_fsm_time(ID, Key, dotted_db_object:get_fsm_time(StrippedObj)),
    strip_save_batch(Objects, S, Now, Acc, ETS).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Try to remove elements from Non-Stripped Keys
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Used periodically to see which non-stripped keys can be stripped.
-spec read_strip_write({[{key(),vv()}], [{dot(), dict:dict()}]}, state()) -> {[{key(),vv()}], [{dot(), dict:dict()}]}.
read_strip_write({Deletes, Writes}, State) ->
    Now = os:timestamp(),
    {Stripped, NotStripped} = split_deletes(Deletes, State, {[],[]}),
    Deletes2 = strip_maybe_save_delete_batch(Stripped, State, Now) ++ NotStripped,
    Writes2 = compute_writes_NSK(Writes, State, [], [], Now),
    {Deletes2, Writes2}.

%% Take care of NSK deletes

split_deletes([], _State, Acc) -> Acc;
split_deletes([{Key, Ctx} | Deletes], State, {Stripped, NotStripped}) ->
    case strip_context(Ctx,State#state.clock) of
        [] -> 
            case read_one_key(Key, State) of
                0   ->
                    ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
                    ets_set_strip_time(State#state.atom_id, Key, os:timestamp()),
                    split_deletes(Deletes, State, {Stripped, NotStripped});
                Obj ->
                    split_deletes(Deletes, State, {[{Key, Obj}|Stripped], NotStripped})
            end;
        VV ->
            split_deletes(Deletes, State, {Stripped, [{Key, VV}|NotStripped]})
    end.

-spec strip_context(vv(), bvv()) -> vv().
strip_context(Context, NodeClock) ->
    FunFilter =
        fun (Id, Counter) ->
            {Base,_Dots} = swc_node:get(Id, NodeClock),
            Counter > Base
        end,
    swc_vv:filter(FunFilter, Context).


strip_maybe_save_delete_batch(O,S,Now) -> strip_maybe_save_delete_batch(O,S,Now,true).
strip_maybe_save_delete_batch(Objects, State, Now, ETS) ->
    strip_maybe_save_delete_batch(Objects, State, Now, {[],[]}, ETS).

strip_maybe_save_delete_batch([], State, _Now, {NSK, StrippedObjects}, _ETS) ->
    ok = dotted_db_storage:write_batch(State#state.storage, StrippedObjects),
    NSK;
strip_maybe_save_delete_batch([{Key={_,_}, Obj} | Objects], State, Now, {NSK, StrippedObjects}, ETS) ->
    % removed unnecessary causality from the object, based on the current node clock
    StrippedObj = dotted_db_object:strip(State#state.clock, Obj),
    {Values, Context} = dotted_db_object:get_container(StrippedObj),
    Values2 = [{D,V} || {D,V} <- Values, V =/= ?DELETE_OP],
    StrippedObj2 = dotted_db_object:set_container({Values2, Context}, StrippedObj),
    % the resulting object is one of the following options:
    %  0 * it has no value but has causal history -> it's a delete, but still must be persisted
    %  1 * it has no value and no causal history -> can be deleted
    %  2 * has values, with causal context -> it's a normal write and we should persist
    %  3 * has values, but no causal context -> it's the final form for this write
    Acc = case {Values2, Context} of
        {[],[]} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 0),
            ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
            ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
            ETS andalso notify_strip_delete_latency(ets_get_write_time(State#state.atom_id, Key), Now),
            ETS andalso ets_set_fsm_time(State#state.atom_id, Key, dotted_db_object:get_fsm_time(StrippedObj)),
            ETS andalso ets_set_dots(State#state.atom_id, Key, []),
            {NSK,                   [{delete, Key}|StrippedObjects]};
        {_ ,[]} ->
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 1),
            ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_WRITE_STRIP),
            ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
            ETS andalso notify_strip_write_latency(ets_get_write_time(State#state.atom_id, Key), Now),
            ETS andalso ets_set_fsm_time(State#state.atom_id, Key, dotted_db_object:get_fsm_time(StrippedObj)),
            ETS andalso ets_set_dots(State#state.atom_id, Key, get_value_dots_for_ets(StrippedObj)),
            {NSK,                   [{put, Key, StrippedObj2}|StrippedObjects]};
        {[],_CC} ->
            {[{Key, Context}|NSK],  StrippedObjects};
        {_ ,_CC} ->
            {[{Key, Context}|NSK],  StrippedObjects}
    end,
    strip_maybe_save_delete_batch(Objects, State, Now, Acc, ETS).


%% Take care of NSK writes

compute_writes_NSK([], State, Batch, NSK, _Now) ->
    ok = dotted_db_storage:write_batch(State#state.storage, Batch),
    NSK;
compute_writes_NSK([{NodeID, Dict} |Tail], State, Batch, NSK, Now) ->
    {DelDots, SaveBatch} = dict:fold(fun(Dot, Key, Acc) -> dictNSK(Dot, Key, Acc, State, Now) end, {[],[]}, Dict),
    NewDict = remove_stripped_writes_NSK(DelDots, Dict),
    case dict:size(NewDict) of
        0 -> compute_writes_NSK(Tail, State, SaveBatch++Batch, NSK, Now);
        _ -> compute_writes_NSK(Tail, State, SaveBatch++Batch, [{NodeID, NewDict}| NSK], Now)
    end.

dictNSK(Dot, {Key, undefined}, {Del, Batch}, State, Now) ->
    case read_one_key(Key, State) of
        0   ->
            ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
            ets_set_strip_time(State#state.atom_id, Key, os:timestamp()),
            {[Dot|Del], Batch};
        Obj ->
            dictNSK2(Dot, {Key, Obj}, {Del,Batch}, State, Now, true)
    end;
dictNSK(Dot, {Key, Ctx}, {Del, Batch}, State, Now) ->
    case strip_context(Ctx, State#state.clock) of
        [] ->
            case read_one_key(Key, State) of
                0 ->
                    ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
                    ets_set_strip_time(State#state.atom_id, Key, os:timestamp()),
                    {[Dot|Del], Batch};
                Obj ->
                    dictNSK2(Dot, {Key, Obj}, {Del,Batch}, State, Now, true)
            end;
        _V ->
            % case random:uniform() < 0.05 of
            %     true  -> lager:info("STRIPPPPPPPPPPPPP:\nClock:~p\nCtx: ~p\n", [State#state.clock, V]);
            %     false -> ok
            % end,
            {Del, Batch} %% not stripped yet; keep in the dict
    end.

dictNSK2(Dot, {Key, Obj}, {Del, Batch}, State, Now, ETS) ->
    % removed unnecessary causality from the object, based on the current node clock
    StrippedObj = dotted_db_object:strip(State#state.clock, Obj),
    {Values, Context} = dotted_db_object:get_container(StrippedObj),
    Values2 = [{D,V} || {D,V} <- Values, V =/= ?DELETE_OP],
    StrippedObj2 = dotted_db_object:set_container({Values2, Context}, StrippedObj),
    % the resulting object is one of the following options:
    %  0 * it has no value but has causal history -> it's a delete, but still must be persisted
    %  1 * it has no value and no causal history -> can be deleted
    %  2 * has values, with causal context -> it's a normal write and we should persist
    %  3 * has values, but no causal context -> it's the final form for this write
    case {Values2, Context} of
        {[],[]} -> % do the real delete
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 0),
            ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
            ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
            ETS andalso notify_strip_delete_latency(ets_get_write_time(State#state.atom_id, Key), Now),
            ETS andalso ets_set_fsm_time(State#state.atom_id, Key, dotted_db_object:get_fsm_time(StrippedObj)),
            ETS andalso ets_set_dots(State#state.atom_id, Key, []),
            {[Dot|Del], [{delete, Key}|Batch]};
        {_ ,[]} -> % write to disk without the version vector context
            ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 1),
            ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_WRITE_STRIP),
            ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
            ETS andalso notify_strip_write_latency(ets_get_write_time(State#state.atom_id, Key), Now),
            ETS andalso ets_set_fsm_time(State#state.atom_id, Key, dotted_db_object:get_fsm_time(StrippedObj)),
            ETS andalso ets_set_dots(State#state.atom_id, Key, get_value_dots_for_ets(StrippedObj)),
            {[Dot|Del], [{put, Key, StrippedObj2}|Batch]};
        {_,_} ->
            {Del, Batch} %% not stripped yet; keep in the dict
    end.

remove_stripped_writes_NSK([], Dict) -> Dict;
remove_stripped_writes_NSK([H|T], Dict) ->
    NewDict = dict:erase(H, Dict),
    remove_stripped_writes_NSK(T, NewDict).

read_one_key(Key={_,_}, State) ->
    case dotted_db_storage:get(State#state.storage, Key) of
        {error, not_found} ->
            0;
        {error, Error} ->
            % some unexpected error
            lager:error("Error reading a key from storage: ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            0;
        Obj ->
            Obj
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Add elements to Non-Stripped Keys
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_keys_to_NSK([], NSK) -> NSK;
add_keys_to_NSK([{Key, Object}|Tail], NSK) ->
    NSK2 = add_key_to_NSK(Key, Object, NSK),
    add_keys_to_NSK(Tail, NSK2).

% @doc Add a replicated key and context to the list of non-stripped-keys
add_key_to_NSK(Key, Object, NSK) ->
    add_key_to_NSK2(Key, dotted_db_object:get_container(Object), NSK).

add_key_to_NSK2(_, {[],[]}, NSK) -> NSK;
add_key_to_NSK2(Key, {[],Ctx}, {Del,Wrt}) ->
    {[{Key, Ctx}|Del], Wrt};
add_key_to_NSK2(Key, {DotValues,Ctx}, NSK) ->
    KeyDots = [{Key, Dot, Ctx} || {Dot,_} <- DotValues],
    add_writes_to_NSK(KeyDots, NSK).

add_writes_to_NSK([], NSK) -> NSK;
add_writes_to_NSK([Head={_,_,_} | Tail], {Del,Wrt}) ->
    Wrt2 = add_one_write_to_NSK(Head, Wrt),
    add_writes_to_NSK(Tail, {Del,Wrt2}).


add_keys_from_dotkeymap_to_NSK([], NSK) -> NSK;
add_keys_from_dotkeymap_to_NSK([{NodeId, DotKeys}|Tail], {Del,Wrt}) ->
    Wrt2 = lists:foldl(
             fun({Dot,Key}, Acc) ->
                     add_one_write_to_NSK({Key, {NodeId, Dot}, undefined}, Acc)
             end,
             Wrt,
             DotKeys),
    % Wrt2 = add_one_write_to_NSK({Key, {NodeID, Base+1}, undefined}, Wrt),
    add_keys_from_dotkeymap_to_NSK(Tail, {Del,Wrt2}).


add_one_write_to_NSK({Key, {NodeID,Counter}, Context}, []) ->
    [{NodeID, dict:store(Counter, {Key, Context}, dict:new())}];
add_one_write_to_NSK({Key, {NodeID, Counter}, Context}, [{NodeID2, Dict}|Tail])
    when NodeID =:= NodeID2 andalso Counter =/= -1 ->
    Dict2 =  dict:store(Counter, {Key, Context}, Dict),
    [{NodeID, Dict2} | Tail];
add_one_write_to_NSK(KV={_, {NodeID,_}, _}, [H={NodeID2, _}|Tail])
    when NodeID =/= NodeID2  ->
    [H | add_one_write_to_NSK(KV, Tail)].



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Recovering vnode saves multiples objects from peers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% For recovering keys remotely after a vnode crash/failure (with lost key-values)
fill_strip_save_kvs(Objects, RemoteClock, LocalClock, State, Now) ->
    fill_strip_save_kvs(Objects, RemoteClock, LocalClock, State, Now, {[],[]}, true).

fill_strip_save_kvs([], _, _, State, _Now, {NSK, StrippedObjects}, _ETS) ->
    ok = dotted_db_storage:write_batch(State#state.storage, StrippedObjects),
    {State#state.clock, State#state.dotkeymap, NSK};
fill_strip_save_kvs([{Key={_,_}, Object} | Objects], RemoteClock, LocalClock, State, Now, {NSK, StrippedObjects}, ETS) ->
    % fill the Object with the sending node clock
    FilledObject = dotted_db_object:fill(Key, RemoteClock, Object),
    % get and fill the causal history of the local key
    DiskObject = guaranteed_get(Key, State#state{clock=LocalClock}),
    % synchronize both objects
    FinalObject = dotted_db_object:sync(FilledObject, DiskObject),
    % test if the FinalObject has newer information
    case (not dotted_db_object:equal_values(FinalObject, DiskObject)) orelse
         (dotted_db_object:get_values(FinalObject)==[] andalso dotted_db_object:get_values(DiskObject)==[]) of
        false -> fill_strip_save_kvs(Objects, RemoteClock, LocalClock, State, Now, {NSK, StrippedObjects}, ETS);
        true ->
            % add each new dot to our node clock
            StateNodeClock = dotted_db_object:add_to_node_clock(State#state.clock, FinalObject),
            % add new keys to the Dot-Key Mapping
            DKM = swc_dotkeymap:add_objects(State#state.dotkeymap, [{Key, dotted_db_object:get_container(FinalObject)}]),
            % removed unnecessary causality from the object, based on the current node clock
            StrippedObject = dotted_db_object:strip(State#state.clock, FinalObject),
            {Values, Context} = dotted_db_object:get_container(StrippedObject),
            Values2 = [{D,V} || {D,V} <- Values, V =/= ?DELETE_OP],
            StrippedObject2 = dotted_db_object:set_container({Values2, Context}, StrippedObject),
            % the resulting object is one of the following options:
            %   * it has no value and no causal history -> can be deleted
            %   * it has no value but has causal history -> it's a delete, but still must be persisted
            %   * has values, with causal context -> it's a normal write and we should persist
            %   * has values, but no causal context -> it's the final form for this write
            Acc = case {Values2, Context} of
                {[],[]} ->
                    ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 0),
                    ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_STRIP),
                    ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
                    ETS andalso notify_strip_delete_latency(Now, Now),
                    ETS andalso ets_set_dots(State#state.atom_id, Key, []),
                    {NSK,                         [{delete, Key}|StrippedObjects]};
                {_ ,[]} ->
                    ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, 1),
                    ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_WRITE_STRIP),
                    ETS andalso ets_set_strip_time(State#state.atom_id, Key, Now),
                    ETS andalso notify_strip_write_latency(Now, Now),
                    ETS andalso ets_set_dots(State#state.atom_id, Key, get_value_dots_for_ets(StrippedObject2)),
                    {NSK,                         [{put, Key, StrippedObject2}|StrippedObjects]};
                {[],_CC} ->
                    ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, length(Context)),
                    ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_DELETE_NO_STRIP),
                    ETS andalso ets_set_dots(State#state.atom_id, Key, get_value_dots_for_ets(StrippedObject2)),
                    {[{Key, StrippedObject2}|NSK], [{put, Key, StrippedObject2}|StrippedObjects]};
                {_ ,_CC} ->
                    ?STAT_ENTRIES andalso dotted_db_stats:notify({histogram, entries_per_clock}, length(Context)+1),
                    ETS andalso ets_set_status(State#state.atom_id, Key, ?ETS_WRITE_NO_STRIP),
                    ETS andalso ets_set_dots(State#state.atom_id, Key, get_value_dots_for_ets(StrippedObject2)),
                    {[{Key, StrippedObject2}|NSK], [{put, Key, StrippedObject2}|StrippedObjects]}
            end,
            ETS andalso notify_write_latency(dotted_db_object:get_fsm_time(StrippedObject2), Now),
            ETS andalso ets_set_write_time(State#state.atom_id, Key, Now),
            ETS andalso ets_set_fsm_time(State#state.atom_id, Key, dotted_db_object:get_fsm_time(StrippedObject2)),
            fill_strip_save_kvs(Objects, RemoteClock, LocalClock, State#state{dotkeymap=DKM, clock=StateNodeClock}, Now, Acc, ETS)
    end.


is_watermark_up_to_date({WM,_}) ->
    (orddict:size(WM) =:= (?REPLICATION_FACTOR*2)-1) andalso
    is_watermark_up_to_date2(WM).

is_watermark_up_to_date2([]) -> true;
is_watermark_up_to_date2([{_,V}|T]) ->
    case orddict:size(V) =:= (?REPLICATION_FACTOR*2)-1 of
        true -> is_watermark_up_to_date2(T);
        false -> false
    end.


new_vnode_id(Index) ->
    % generate a new vnode ID for now
    dotted_db_utils:maybe_seed(),
    % get a random index withing the length of the list
    {Index, random:uniform(999999999999)}.

create_ets_all_keys(NewVnodeID) ->
    % create the ETS for this vnode
    AtomID = get_ets_id(NewVnodeID),
    _ = ((ets:info(AtomID) =:= undefined) andalso
            ets:new(AtomID, [named_table, public, set, {write_concurrency, false}])),
    AtomID.

delete_ets_all_keys(#state{atom_id=AtomID}) ->
    _ = ((ets:info(AtomID) =:= undefined) andalso ets:delete(AtomID)),
    true.

-spec get_ets_id(any()) -> atom().
get_ets_id(Id) ->
    list_to_atom(lists:flatten(io_lib:format("~p", [Id]))).

sync_clocks(LocalClock, RemoteClock, RemoteIndex) ->
    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    RemoteClock2 = orddict:filter(fun ({Index,_},_) -> Index == RemoteIndex end, RemoteClock),
    swc_node:merge(LocalClock, RemoteClock2).


update_watermark_after_sync(MyWatermark, RemoteWatermark, MyIndex, RemoteIndex, MyClock, RemoteClock) ->
    % update my watermark with my what I know, based on my node clock
    MyWatermark2 = orddict:fold(
            fun (Vnode={Index,_}, _, Acc) ->
                    case Index == MyIndex of
                        false -> Acc;
                        true -> swc_watermark:update_peer(Acc, Vnode, MyClock)
                    end
            end, MyWatermark, MyClock),
    MyWatermark3 = orddict:fold(
            fun (Vnode={Index,_}, _, Acc) ->
                    case Index == RemoteIndex of
                        false -> Acc;
                        true -> swc_watermark:update_peer(Acc, Vnode, RemoteClock)
                    end
            end, MyWatermark2, RemoteClock),
    % update the watermark to reflect what the asking peer has about its peers
    swc_watermark:left_join(MyWatermark3, RemoteWatermark).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ETS functions that store some stats and benchmark info
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% @doc Returns a pair: first is the number of keys present in storage,
% the second is the number of keys completely deleted from storage.
ets_get_all_keys(State) ->
    ets:foldl(fun
        ({Key,St,_,_,_,_}, {Others, Deleted}) when St =:= ?ETS_DELETE_STRIP -> {Others, [Key|Deleted]};
        ({Key,St,_,_,_,_}, {Others, Deleted}) when St =/= ?ETS_DELETE_STRIP -> {[Key|Others], Deleted}
    end, {[],[]}, State#state.atom_id).
    % ets_get_issued_written(State#state.atom_id) ++
    % ets_get_final_written(State#state.atom_id) ++
    % ets_get_issued_deleted(State#state.atom_id).

ets_set_status(Id, Key, Status)     -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {2, Status}).
% ets_set_strip_time(_, _, undefined) -> true;
ets_set_strip_time(Id, Key, Time)   -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {3, Time}).
% ets_set_write_time(_, _, undefined) -> true;
ets_set_write_time(Id, Key, Time)   -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {4, Time}).
ets_set_fsm_time(_, _, undefined)   -> true;
ets_set_fsm_time(Id, Key, Time)     -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {5, Time}).
ets_set_dots(Id, Key, Dots)         -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {6, Dots}).

notify_write_latency(undefined, _WriteTime) ->
    lager:warning("Undefined FSM write time!!!!!!!!"), ok;
notify_write_latency(_FSMTime, undefined) ->
    % lager:warning("Undefined write time!!!!!!!!"),
    ok;
notify_write_latency(FSMTime, WriteTime) ->
    case ?STAT_WRITE_LATENCY of
        false -> ok;
        true ->
            Delta = timer:now_diff(WriteTime, FSMTime)/1000,
            dotted_db_stats:notify({gauge, write_latency}, Delta)
    end.

notify_strip_write_latency(undefined, _StripTime) -> ok;
notify_strip_write_latency(WriteTime, StripTime) ->
    case ?STAT_STRIP_LATENCY of
        false -> ok;
        true ->
            Delta = timer:now_diff(StripTime, WriteTime)/1000,
            dotted_db_stats:notify({gauge, strip_write_latency}, Delta)
    end.

notify_strip_delete_latency(undefined, _StripTime) -> ok;
notify_strip_delete_latency(WriteTime, StripTime) ->
    case ?STAT_STRIP_LATENCY of
        false -> ok;
        true ->
            Delta = timer:now_diff(StripTime, WriteTime)/1000,
            dotted_db_stats:notify({gauge, strip_delete_latency}, Delta)
    end.

ensure_tuple(Id, Key) ->
    U = undefined,
    not ets:member(Id, Key) andalso ets:insert(Id, {Key,U,U,U,U,U}).

% ets_get_status(Id, Key)     -> ets:lookup_element(Id, Key, 2).
% ets_get_strip_time(Id, Key) -> ets:lookup_element(Id, Key, 3).
ets_get_write_time(Id, Key) -> ensure_tuple(Id, Key), ets:lookup_element(Id, Key, 4).
ets_get_fsm_time(Id, Key)   -> ensure_tuple(Id, Key), ets:lookup_element(Id, Key, 5).
% ets_get_dots(Id, Key)       -> ets:lookup_element(Id, Key, 6).

ets_get_issued_deleted(Id)  ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_DELETE_NO_STRIP}], ['$1'] }]).
ets_get_actual_deleted(Id)  ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_DELETE_STRIP}], ['$1'] }]).
ets_get_issued_written(Id)  ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_WRITE_NO_STRIP}], ['$1'] }]).
ets_get_final_written(Id)   ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_WRITE_STRIP}], ['$1'] }]).

compute_strip_latency(Id) ->
    ets:foldl(fun
        ({_,_,undefined,_,_,_}, Acc) -> Acc; ({_,_,_,undefined,_,_}, Acc) -> Acc;
        ({_,_,Strip,Write,_,_}, Acc) -> [timer:now_diff(Strip, Write)/1000 | Acc]
    end, [], Id).

compute_replication_latency(Id) ->
    ets:foldl(fun
        ({_,_,_,_,undefined,_}, Acc) -> Acc; ({_,_,_,undefined,_,_}, Acc) -> Acc;
        ({_,_,_,Write,Fsm,_}, Acc) -> [timer:now_diff(Write, Fsm)/1000 | Acc]
    end, [], Id).

% ets_get_all_dots(EtsId) ->
%     ets:foldl(fun
%         ({Key,?ETS_DELETE_STRIP   ,_,_,_,Dots}, {Others, Deleted}) -> {Others, [{Key,lists:sort(Dots)}|Deleted]};
%         ({Key,?ETS_DELETE_NO_STRIP,_,_,_,Dots}, {Others, Deleted}) -> {Others, [{Key,lists:sort(Dots)}|Deleted]};
%         ({Key,?ETS_WRITE_STRIP    ,_,_,_,Dots}, {Others, Deleted}) -> {[{Key,lists:sort(Dots)}|Others], Deleted};
%         ({Key,?ETS_WRITE_NO_STRIP ,_,_,_,Dots}, {Others, Deleted}) -> {[{Key,lists:sort(Dots)}|Others], Deleted};
%         ({Key,undefined,_,_,_,undefined},       {Others, Deleted}) -> {Others, [{Key,undefined}|Deleted]}
%     end, {[],[]}, EtsId).

storage_get_all_dots(Storage) ->
    Fun = fun({Key, Object}, {Others, Deleted}) ->
        DCC = dotted_db_object:get_container(Object),
        {[{Key,DCC}|Others], Deleted}
    end,
    dotted_db_storage:fold(Storage, Fun, {[],[]}).

get_value_dots_for_ets(Object) ->
    {ValueDots, _Context} = dotted_db_object:get_container(Object),
    ValueDots2 = [{D,V} || {D,V} <- ValueDots, V =/= ?DELETE_OP],
    orddict:fetch_keys(ValueDots2).


%%% Functions for the small in-memory tracking of which peers this node synces since a some node failure

add_removed_vnode_jump_clock(OldVnodeID) ->
    Dict =  case get(jump_clock) of
                undefined -> orddict:new();
                D -> D
            end,
    lager:warning("MEMORY: new retired vnode: ~p\n", [OldVnodeID]),
    put(jump_clock, orddict:store(OldVnodeID, orddict:new(), Dict)).

update_jump_clock(SyncPeerIndex) ->
    case get(jump_clock) of
        undefined -> ok;
        Dict ->
            % case random:uniform() < 0.01 of
            %     true -> lager:warning("for ~p: ~p\n\n", [SyncPeerIndex,Dict]);
            %     false -> ok
            % end,
            D2 = orddict:map(fun (_,PeersCount) -> orddict:update_counter(SyncPeerIndex, 1, PeersCount) end, Dict),
            D3 = orddict:filter(fun (_,PeersCount) ->
                                        PeersCount2 = orddict:filter(fun (_,C) -> C > 50 end, PeersCount),
                                        orddict:size(PeersCount2) < (?REPLICATION_FACTOR-1)*2
                                end, D2),
            % D4 = orddict:filter(fun (_,PeersCount) ->
            %                             PeersCount2 = orddict:filter(fun (_,C) -> C > 50 end, PeersCount),
            %                             orddict:size(PeersCount2) >= (?REPLICATION_FACTOR-1)*2
            %                     end, D2),
            % case orddict:fetch_keys(D4) of
            %     [] -> ok;
            %     Rem -> lager:warning("MEMORY: from ~p deleted: ~p\n", [x99problems, Rem])
            % end,
            put(jump_clock, D3)
    end.

get_old_peers_still_not_synced() ->
    case get(jump_clock) of
        undefined -> [];
        Dict -> orddict:fetch_keys(Dict)
    end.

jump_node_clock_by_index(Clock, CurrentId, Index, Jump) ->
    OldIds = [Id || Id={Idx,_} <- swc_node:ids(Clock) , Idx == Index andalso Id =/= CurrentId],
    lists:foldl(fun (OldId, AccClock) ->
                        {Base,_} = swc_node:get(OldId, AccClock),
                        swc_node:store_entry(OldId, {Base+Jump,0}, AccClock)
                end, Clock, OldIds).

