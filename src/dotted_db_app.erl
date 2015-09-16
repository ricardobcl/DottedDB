-module(dotted_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(INTERVAL, 1000).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case dotted_db_sup:start_link() of
        {ok, Pid} ->

            ok = riak_core:register([{vnode_module, dotted_db_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(dotted_db_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(dotted_db_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(dotted_db, self()),

            dotted_db_stats:add_stats([
                % size (bytes) of the node clock
                {histogram, bvv_size},
                % length of the key log (how many keys)
                {histogram, kl_len},
                % average number of entries in the stripped objects
                % whose keys were removed from the keylog
                {histogram, sync_local_dcc_strip},
                % average number of entries in the stripped objects
                % sent to the destination node
                {histogram, sync_sent_dcc_strip},
                % ratio of total missing keys in a sync, vs the
                % missing keys relevant to the destination node
                {histogram, sync_relevant_ratio},
                % ratio of sent keys and actual missing keys in a sync
                {histogram, sync_hit_ratio},
                % size of the payload (actual data) of the sent objects in a sync
                {histogram, sync_payload_size},
                % size of the metadata of the sent objects in a sync
                {histogram, sync_metadata_size},
                % number of delete requests
                {histogram, deletes_made},
                % number of actual delete in the server
                {histogram, deletes_completed}
            ]),
            dotted_db_stats:start(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
