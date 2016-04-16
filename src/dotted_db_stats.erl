-module(dotted_db_stats).

-behaviour(gen_server).


%% API for gen_server
-export([     start_link/0
            , start_link/1
            , add_stats/1
            , add_stats/2
            , notify/2
            , start_bench/0
            , end_bench/1
            , start/0
            , stop/0
            , new_dir/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("dotted_db.hrl").

-define(WARN_INTERVAL, 1000). % Warn once a second
-define(CURRENT_DIR, "current").
-define(ETS, ets_dotted_db_entropy).

-record(state, {
        bench              = off,
        bench_start        = os:timestamp(),
        stats              = [],
        start_time         = os:timestamp(),
        last_write_time    = os:timestamp(),
        flush_interval     = ?STATS_FLUSH_INTERVAL*1000,
        timer              = undefined,
        active             = false,
        last_warn          = {0,0,0}
}).


%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    start_link([]).

start_link(Stats) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Stats], []).

%% @doc Dynamically adds a list of stats {Type, Name}
add_stats(NewStats) ->
    add_stats(NewStats, "").
%% @doc Dynamically adds a list of stats {Type, Name} with Optional header for csv's
add_stats(NewStats, OptionalHeader) ->
    gen_server:call(?MODULE, {add_stats, NewStats, OptionalHeader}).

notify(Name, 0.0) -> notify(Name, 0);
notify(Name, Value) ->
    gen_server:cast(?MODULE, {notify, Name, Value}).

%% @doc Store the time at which the benchmark started
start_bench() ->
    gen_server:call(?MODULE, start_bench).

%% @doc Store the time at which the benchmark started
end_bench(Args) ->
    gen_server:cast(?MODULE, {end_bench, Args}).

%% @doc Starts the timer that flush the data to disk.
start() ->
    gen_server:call(?MODULE, start).

%% @doc Stops the timer that flush the data to disk.
stop() ->
    gen_server:call(?MODULE, stop).

new_dir() ->
    gen_server:call(?MODULE, new_dir).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Stats]) ->
    %% Create the ETS table for (cumulative?) stats
    _ = create_table(),
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, normal),

    %% Create the stats directory and setups the output file handles for dumping
    %% periodic CSV of histogram results.
    _ = init_stat_files(Stats, "", true),
    %% Register each new stat with folsom.
    _ = [create_stat_folsom(Stat) || Stat <- Stats],

    {ok, #state{ stats = Stats,
                 bench = off,
                 flush_interval = timer:seconds(?STATS_FLUSH_INTERVAL)}}.

%% Synchronous calls
handle_call(start_bench, _From, State) ->
    Now = os:timestamp(),
    {reply, ok, State#state{bench = on, bench_start = Now}};


handle_call(start, _From, State) ->
    %% Schedule next report (repeatedly calls `self() ! report`)
    {ok, Timer} = timer:send_interval(State#state.flush_interval, flush),
    Now = os:timestamp(),
    {reply, ok, State#state{    start_time = Now,
                                last_write_time = Now,
                                timer = Timer,
                                active = true}};

handle_call(stop, _From, State) ->
    %% Cancel timer to flush data to disk
    {ok, cancel} = timer:cancel(State#state.timer),
    %% Flush data to disk
    Now = os:timestamp(),
    _ = process_stats(Now, State),
    {reply, ok, State#state{    last_write_time = Now,
                                timer = undefined,
                                active = false}};

handle_call({add_stats, NewStats, OptionalHeader}, _From, State = #state{stats = CurrentStats}) ->
    %% Create the stats directory and setups the output file handles for dumping
    %% periodic CSV of histogram results.
    _ = init_stat_files(NewStats, OptionalHeader),
    %% Register each new stat with folsom.
    _ = [create_stat_folsom(Stat) || Stat <- NewStats],
    {reply, ok, State#state{stats = CurrentStats ++ NewStats}};

handle_call(new_dir, _From, State) ->
    %% Create a new folder for stats and point ?CURRENT_DIR to it.
    _ = init_stat_files(State#state.stats, "", true),
    {reply, ok, State}.



%% Asynchronous calls

handle_cast({end_bench, _Args}, State=#state{bench = off}) ->
    lager:info("Bench already ended!"),
    {noreply, State};
handle_cast({end_bench, Args}, State=#state{bench = on}) ->
    StartTime = State#state.bench_start,
    EndTime = os:timestamp(),
    Dir = erlang:get(bench_file),
    spawn(fun() -> save_bench_file(StartTime, EndTime, Args, Dir, State#state.start_time) end),
    {noreply, State#state{bench = off}};

%% Ignore notifications if active flag is set to false.
handle_cast({notify,_,_}, State=#state{active = false}) ->
    lager:info("Stats: ignored notification!"),
    {noreply, State};

handle_cast({notify, {histogram, Name}, Value}, State = #state{
                            last_write_time = LWT,
                            flush_interval = FI,
                            timer = Timer,
                            active = true}) ->
    Now = os:timestamp(),
    TimeSinceLastReport = timer:now_diff(Now, LWT) / 1000, %% To get the diff in milliseconds
    TimeSinceLastWarn = timer:now_diff(Now, State#state.last_warn) / 1000,
    NewState = case TimeSinceLastReport > (FI * 2) andalso TimeSinceLastWarn > ?WARN_INTERVAL of
        true ->
            lager:warning("dotted_db_stats has not reported in ~.2f milliseconds\n", [TimeSinceLastReport]),
            {message_queue_len, QLen} = process_info(self(), message_queue_len),
            lager:warning("stats process mailbox size = ~w\n", [QLen]),
            State#state{last_warn = Now};
        false ->
            State
    end,
    case Timer of
        undefined ->
            lager:warning("dotted_db_stats is not flushing received data (start the timer)\n");
        _ -> ok
    end,
    ok = folsom_metrics:notify({histogram, Name}, Value),
    ok = folsom_metrics:notify({units, Name}, {inc, 1}),
    {noreply, NewState};

handle_cast({notify, {counter, Name}, Value}, State = #state{active = true}) ->
    ok = folsom_metrics:notify({counter, Name}, {inc, Value}),
    {noreply, State};

handle_cast({notify, {gauge, Name}, Value}, State = #state{active = true}) ->
    % Values = folsom_metrics:get_metric_value({gauge, StatName}),
    List = case ets:lookup(stats_gauge, Name) of
        [] -> [];
        [{Name, L}] -> L
    end,
    {Mega, Sec, _} = erlang:now(),
    Timestamp = Mega * 1000000 + Sec,
    ets:insert(stats_gauge, {Name, [{Timestamp, Value} | List]}),
    % ok = folsom_metrics:notify({gauge, Name}, Value),
    {noreply, State}.



handle_info(flush, State) ->
    consume_flush_msgs(),
    Now = os:timestamp(),
    _ = process_stats(Now, State),
    {noreply, State#state { last_write_time = Now }};

handle_info(Info, State) ->
    lager:info("~p: unhandled_info: ~p",[?MODULE, Info]),
    {ok, State}.


terminate(_Reason, State) ->
    %% Do the final stats report
    _ = process_stats(os:timestamp(), State),
    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    [ok = file:close(F) || {{cdf_file, _}, F} <- erlang:get()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% ====================================================================
%% Internal functions
%% ====================================================================

create_table() ->
    (ets:info(?ETS) /= undefined) orelse
        ets:new(?ETS, [named_table, public, set, {write_concurrency, true}]).


process_stats(Now, State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = round(timer:now_diff(Now, State#state.start_time) / 1000000),
    Window  = round(timer:now_diff(Now, State#state.last_write_time) / 1000000),
    [begin
         OpAmount = save_histogram(Elapsed, Window, Stat),
         folsom_metrics:notify({units, Stat}, {dec, OpAmount})
     end || {histogram, Stat} <- State#state.stats],
    [save_gauge(Stat) || {gauge, Stat} <- State#state.stats].


%% @doc Write measurement info for a given op to the appropriate CSV. Returns
%% the number of successful and failed stats in this window of time.
save_histogram(Elapsed, Window, StatName) ->
    Stats = folsom_metrics:get_histogram_statistics({histogram, StatName}),
    Units = folsom_metrics:get_metric_value({units, StatName}),
    Line = case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~w, ~w, ~w, ~w, ~w, ~w, ~w, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Units,
                                  proplists:get_value(min, Stats),
                                  proplists:get_value(arithmetic_mean, Stats),
                                  proplists:get_value(median, Stats),
                                  proplists:get_value(95, P),
                                  proplists:get_value(99, P),
                                  proplists:get_value(999, P),
                                  proplists:get_value(max, Stats),
                                  round(Units * proplists:get_value(arithmetic_mean, Stats)),
                                  proplists:get_value(standard_deviation, Stats),
                                  0]);
        false ->
            lager:debug("No data for stat: ~p\n", [StatName]),
            io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n",
                                 [Elapsed,
                                  Window])
    end,
    ok = file:write(erlang:get({csv_file, StatName}), Line),
    Units.

save_gauge(StatName) ->
    % Values = folsom_metrics:get_metric_value({gauge, StatName}),
    ok = case ets:lookup(stats_gauge, StatName) of
        [] -> ok;
        [{StatName, Values}] ->
            ets:delete(stats_gauge, StatName),
            % lager:info("~p: |~p| \n",[StatName, Values]),
            Line = lists:foldl(fun({Time,Value}, Acc) -> Acc++io_lib:format("~w, ~w\n", [Time,Value]) end, [], Values),
            file:write(erlang:get({cdf_file, StatName}), Line)
    end.

save_bench_file(StartTime, EndTime, [SyncTime, ReplFail, NodeKill, StripInterval], Dir, Stime) ->
    dotted_db_utils:maybe_seed(),
    Sec = random:uniform(10)+5,
    lager:info("Ending bench going to sleep ~p seconds!", [Sec]),
    timer:sleep(timer:seconds(Sec)),

    NumKeys = dotted_db:all_keys(),
    {ok, Nvnodes} = application:get_env(riak_core, ring_creation_size),
    ST = round(timer:now_diff(StartTime, Stime) / 1000000),
    ET = round(timer:now_diff(EndTime, Stime) / 1000000),
    Line0 = io_lib:format("Start Time \t\t\t:\t~w\nEnd Time \t\t\t:\t~w\nNumber of Keys \t\t\t:\t~w\nSync Interval \t\t\t:\t~w\nNode Kill Rate \t\t\t:\t~w\n",
                       [ST, ET, NumKeys, SyncTime, NodeKill]),
    Line1 = io_lib:format("Replication Failure Rate \t:\t~w\nNumber of Vnodes \t\t:\t~w\nReplication Factor \t\t:\t~w\n",
                       [ReplFail, Nvnodes, ?REPLICATION_FACTOR]),
    Line2 = io_lib:format("Strip Interval \t\t\t:\t~w\n", [StripInterval]),
    Fname = filename:join([Dir, "bench_file.csv"]),
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, list_to_binary(Line0 ++ Line1 ++ Line2)).


create_new_dir() ->
    TestDir = get_stats_dir(new_dir_name()),
    Link = get_stats_dir(?CURRENT_DIR),
    ok = filelib:ensure_dir(filename:join(TestDir, "foobar")),
    [] = os:cmd(lists:flatten(io_lib:format("rm -f ~s; ln -sf ~s ~s", [Link, TestDir, Link]))).

get_stats_dir() ->
    {ok, CWD} = file:get_cwd(),
    % WDir = filename:dirname(CWD),
    filename:join([CWD, "data/stats"]).

get_stats_dir(?CURRENT_DIR) ->
    Dir = get_stats_dir(),
    filename:join([Dir, ?CURRENT_DIR]);
get_stats_dir(Name) ->
    Dir = get_stats_dir(),
    DirAbs = filename:absname(Dir),
    filename:join([DirAbs, Name]).

new_dir_name() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    lists:flatten(io_lib:format("~w~2..0w~2..0w_~2..0w~2..0w~2..0w", [Y, M, D, H, Min, S])).



%% @doc Create a stat with folsom
create_stat_folsom({gauge, _Name}) ->
    % ok = folsom_metrics:new_gauge({gauge, Name});
    (ets:info(stats_gauge) =:= undefined) andalso
        ets:new(stats_gauge, [named_table, public, set, {write_concurrency, true}]);
create_stat_folsom({counter, Name}) ->
    ok = folsom_metrics:new_counter({counter, Name});
create_stat_folsom({histogram, Name}) ->
    ok = folsom_metrics:new_histogram({histogram, Name}, slide, ?STATS_FLUSH_INTERVAL),
    ok = folsom_metrics:new_counter({units, Name}).

%% @doc Create the stats directory and setups the output file handles for dumping
%% periodic CSV of histogram results.
init_stat_files(Stats, Header) ->
    init_stat_files(Stats, Header, false).
init_stat_files(Stats, Header, NewDir) ->
    TestDir = get_stats_dir(?CURRENT_DIR),
    _ = case (not filelib:is_dir(TestDir)) orelse NewDir of
        true -> create_new_dir();
        false -> ok
    end,
    erlang:put(bench_file, TestDir),
    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, Name}, histogram_csv_file(Name,TestDir,Header)) || {histogram, Name} <- Stats],
    %% Setup output file handles for dumping gauge value for the CDF (Cumulative distribution function).
    [erlang:put({cdf_file, Name}, gauge_cdf_file(Name,TestDir,Header))     || {gauge, Name} <- Stats].

%% @doc Setups a histogram file for a stat.
histogram_csv_file(Label, Dir, Header) ->
    Fname = normalize_label(Label) ++ "_hist.csv",
    Fname2 = filename:join([Dir, Fname]),
    {ok, F} = file:open(Fname2, [raw, binary, write]),
    ok = file:write(F, list_to_binary(Header++"\nelapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, total_sum, std_dev, errors\n")),
    F.

%% @doc Setups a csv file for a gauges.
gauge_cdf_file(Label, Dir, Header) ->
    Fname = normalize_label(Label) ++ "_gauge.csv",
    Fname2 = filename:join([Dir, Fname]),
    {ok, F} = file:open(Fname2, [raw, binary, write]),
    ok = file:write(F, list_to_binary(Header++"\ntimestamp, value\n")),
    F.

normalize_label(Label) when is_list(Label) ->
    replace_special_chars(Label);
normalize_label(Label) when is_binary(Label) ->
    normalize_label(binary_to_list(Label));
normalize_label(Label) when is_integer(Label) ->
    normalize_label(integer_to_list(Label));
normalize_label(Label) when is_atom(Label) ->
    normalize_label(atom_to_list(Label));
normalize_label(Label) when is_tuple(Label) ->
    Parts = [normalize_label(X) || X <- tuple_to_list(Label)],
    string:join(Parts, "-").

replace_special_chars([H|T]) when
      (H >= $0 andalso H =< $9) orelse
      (H >= $A andalso H =< $Z) orelse
      (H >= $a andalso H =< $z) ->
    [H|replace_special_chars(T)];
replace_special_chars([_|T]) ->
    [$-|replace_special_chars(T)];
replace_special_chars([]) ->
    [].

consume_flush_msgs() ->
    receive
        flush ->
            consume_flush_msgs()
    after 0 ->
            ok
    end.
