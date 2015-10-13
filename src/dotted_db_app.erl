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


            Port = case app_helper:get_env(dotted_db, protocol_port) of
                N when is_integer(N)    -> N;
                _                       -> 0
            end,
            {ok, _} = ranch:start_listener(the_socket, 10,
                            ranch_tcp, [{port, Port}], dotted_db_socket, []),

            dotted_db_stats:add_stats([
                % size (bytes) of the node clock
                {histogram, bvv_size},
                % number of missing dots from the node clock
                {histogram, bvv_missing_dots},
                % length of the key log (how many keys)
                {histogram, kl_len},
                % size of the key log
                {histogram, kl_size},
                % number of non-stripped-keys in the node
                {histogram, nsk_number},
                % size of non-stripped-keys in the node
                {histogram, nsk_size},
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
                % number of missing objects sent
                {histogram, sync_sent_missing},
                % number of (non-stripped) delete requests
                {histogram, deletes_incomplete},
                % number of actual delete in the server
                {histogram, deletes_completed},
                % number of (non-stripped) write requests
                {histogram, write_incomplete},
                % number of stripped write in the server
                {histogram, write_completed}
            ]),
            dotted_db_stats:start(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
