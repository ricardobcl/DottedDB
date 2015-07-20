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
            application:ensure_all_started(exometer),

            ok = riak_core:register([{vnode_module, dotted_db_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(dotted_db_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(dotted_db_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(dotted_db, self()),

            % EntryRoute = {["dotted_db", "ping", client], dotted_db_wm_ping, []},
            % webmachine_router:add_route(EntryRoute),

            % {ok,_} = dotted_db_stats:start_link([{histogram, bvv_size}]),
            dotted_db_stats:add_stats([{histogram, bvv_size}]),
            dotted_db_stats:add_stats([{histogram, kl_len}]),

            start_stats(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.

start_stats() ->

    %% Graphite
    % {ok, Host} = inet:gethostname(),
    ReportOptions = [{connect_timeout, 5000},
                     {prefix, "DottedDB."},
                     % {host, "carbon.hostedgraphite.com"},
                     {host, "localhost"},
                     {port, 2003},
                     {api_key, "dotted_db_stats_ric"}],
    
    try 
        ok = exometer_report:add_reporter(exometer_report_graphite, ReportOptions),
        % VM memory.
        % total = processes + system.
        % processes = used by Erlang processes, their stacks and heaps.
        % system = used but not directly related to any Erlang process.
        % atom = allocated for atoms (included in system).
        % binary = allocated for binaries (included in system).
        % ets = allocated for ETS tables (included in system).
        ok = exometer:new([erlang, memory],
                          {function, erlang, memory, ['$dp'], value,
                           [total, processes, system, atom, binary, ets]}),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [erlang, memory],
                                       [total, processes, system, atom, binary,
                                        ets], ?INTERVAL),
    
        % process_count = current number of processes.
        % port_count = current number of ports.
        ok = exometer:new([erlang, system],
                          {function, erlang, system_info, ['$dp'], value,
                           [process_count, port_count]}),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [erlang, system],
                                       [process_count, port_count], ?INTERVAL),
    
        % The number of processes that are ready to run on all available run queues.
        ok = exometer:new([erlang, statistics],
                          {function, erlang, statistics, ['$dp'], value,
                           [run_queue]}),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [erlang, statistics],
                                       [run_queue], ?INTERVAL),
    
    
        % 
        ok = exometer:new([dotted_db, glc, entries, length], histogram),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [dotted_db, glc, entries, length],
                                       [min, max, median, mean, 95, 99, 999], 
                                       ?INTERVAL),
        % 
        ok = exometer:new([dotted_db, sync, total], counter),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [dotted_db, sync, total],
                                       value, 
                                       ?INTERVAL),
    
        ok = exometer:new([dotted_db, sync, relevant_keys], histogram),
        ok = exometer_report:subscribe(exometer_report_graphite,
                                       [dotted_db, sync, relevant_keys],
                                       [min, max, median, mean, 95, 99, 999], 
                                       ?INTERVAL)
    of
        _ -> true
    catch
        throw:Throw -> 
            lager:warning("Throw: ~p!", [Throw]);
        exit:Exit   -> 
            lager:warning("Exit: ~p!",  [Exit]);
        error:Error -> 
            lager:warning("Error: ~p!", [Error]);
        _:_         ->
            lager:warning("Problem with exometer!")
    end,

    ok.
