-module(dotted_db_stats).

-behaviour(gen_server).


%% API
-export([     start_link/0
            , start_link/1
            , add_stats/1
            , notify/2
            , start/0
            , stop/0
            , new_dir/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("dotted_db.hrl").

-define(FLUSH_INTERVAL, 10). % 10 seconds
-define(FLUSH_INTERVAL_MS, 10000). % 10000 milliseconds
-define(WARN_INTERVAL, 1000). % Warn once a second
-define(CURRENT_DIR, "current").


-record(state, { stats              = [],
                 start_time         = os:timestamp(),
                 last_write_time    = os:timestamp(),
                 flush_interval     = ?FLUSH_INTERVAL*1000,
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
    % gen_server:start_link({global, ?MODULE}, ?MODULE, [Stats], []).
    global:trans({?MODULE, ?MODULE}, fun() ->
        case gen_server:start_link({global, ?MODULE}, ?MODULE, [Stats], []) of
            {ok, Pid} -> 
                {ok, Pid};
            {error, {already_started, Pid}} ->  
                link(Pid),
                {ok, Pid};
            Else -> Else
        end
    end).

%% @doc Dynamically adds a list of stats {Type, Name}
add_stats(NewStats) ->
    ?PRINT(NewStats),
    gen_server:call({global, ?MODULE}, {add_stats, NewStats}).

notify(Name, Value) ->
    gen_server:cast({global, ?MODULE}, {notify, Name, Value}).

%% @doc Starts the timer that flush the data to disk.
start() ->
    gen_server:call({global, ?MODULE}, start).

%% @doc Stops the timer that flush the data to disk.
stop() ->
    gen_server:call({global, ?MODULE}, stop).

new_dir() ->
    gen_server:call({global, ?MODULE}, new_dir).







%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Stats]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, high),
    
    % %% Spin up folsom
    % folsom:start(),

    % %% Create the stats directory and setups the output file handles for dumping 
    % %% periodic CSV of histogram results.
    % init_histogram_files(Stats),

    % %% Setup a histogram and counter for each operation -- we only track stats
    % %% on successful operations
    % [begin
    %      folsom_metrics:new_histogram({stats, Op}, slide, ReportInterval),
    %      folsom_metrics:new_counter({units, Op})
    %  end || Op <- Stats],

    % %% Schedule next write/reset of data
    % ReportIntervalSec = timer:seconds(?FLUSH_INTERVAL),

    %% Create the stats directory and setups the output file handles for dumping 
    %% periodic CSV of histogram results.
    init_histogram_files(Stats),
    %% Register each new stat with folsom.
    [create_stat_folsom(Stat) || Stat <- Stats],

    {ok, #state{ stats = Stats,
                 flush_interval = timer:seconds(?FLUSH_INTERVAL)}}.

%% Synchronous calls
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
    process_stats(Now, State),
    {reply, ok, State#state{    last_write_time = Now,
                                timer = undefined,
                                active = false}};

handle_call({add_stats, NewStats}, _From, State = #state{stats = CurrentStats}) ->
    ?PRINT("add_stats"),
    ?PRINT(NewStats),
    %% Create the stats directory and setups the output file handles for dumping 
    %% periodic CSV of histogram results.
    init_histogram_files(NewStats),
    %% Register each new stat with folsom.
    [create_stat_folsom(Stat) || Stat <- NewStats],
    {reply, ok, State#state{stats = CurrentStats ++ NewStats}};

handle_call(new_dir, _From, State) ->
    %% Create a new folder for stats and point ?CURRENT_DIR to it.
    init_histogram_files(State#state.stats, true),
    {reply, ok, State}.



%% Asynchronous calls

%% Ignore notifications if active flag is set to false.
handle_cast({notify,_,_}, State=#state{active = false}) ->
    lager:debug("Stats: ignored notification!"),
    {noreply, State};

handle_cast({notify, {histogram, Name}, Value}, State = #state{
                            last_write_time = LWT,
                            flush_interval = FI,
                            timer = Timer,
                            active = true}) ->
    Now = os:timestamp(),
    TimeSinceLastReport = timer:now_diff(Now, LWT) / 1000, %% To get the diff in seconds
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
    folsom_metrics:notify({histogram, Name}, Value),
    folsom_metrics:notify({units, Name}, {inc, 1}),
    {noreply, NewState};

handle_cast({notify, {counter, Name}, Value}, State = #state{active = true}) ->
    folsom_metrics:notify({counter, Name}, {inc, Value}),
    {noreply, State}.


handle_info(flush, State) ->
    consume_flush_msgs(),
    Now = os:timestamp(),
    process_stats(Now, State),
    {noreply, State#state { last_write_time = Now }}.

terminate(_Reason, State) ->
    %% Do the final stats report
    process_stats(os:timestamp(), State),
    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.











%% ====================================================================
%% Internal functions
%% ====================================================================

process_stats(Now, State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = timer:now_diff(Now, State#state.start_time) / 1000000,
    Window  = timer:now_diff(Now, State#state.last_write_time) / 1000000,
    [begin
         OpAmount = save_histogram(Elapsed, Window, Stat),
         folsom_metrics_counter:dec({units, Stat}, OpAmount)
     end || {histogram, Stat} <- State#state.stats].


%% @doc Write measurement info for a given op to the appropriate CSV. Returns
%% the number of successful and failed stats in this window of time.
save_histogram(Elapsed, Window, Op) ->
    Stats = folsom_metrics:get_histogram_statistics({histogram, Op}),
    Units = folsom_metrics:get_metric_value({units, Op}),
    case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~w, ~w, ~w, ~w, ~w, ~w\n",
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
                                  0]);
        false ->
            lager:warning("No data for op: ~p\n", [Op]),
            Line = io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, 0\n",
                                 [Elapsed,
                                  Window])
    end,
    ok = file:write(erlang:get({csv_file, Op}), Line),
    Units.

create_new_dir() ->
    TestDir = get_stats_dir(new_dir_name()),
    Link = get_stats_dir(?CURRENT_DIR),
    ok = filelib:ensure_dir(filename:join(TestDir, "foobar")),
    [] = os:cmd(lists:flatten(io_lib:format("rm -f ~s; ln -sf ~s ~s", [Link, TestDir, Link]))).

get_stats_dir() ->
    {ok, CWD} = file:get_cwd(),
    WDir = filename:dirname(CWD),
    filename:join([WDir, "stats"]).

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
% create_stat_folsom(Name, spiral) ->
%     folsom_metrics:new_spiral({spiral, Name});
create_stat_folsom({counter, Name}) ->
    folsom_metrics:new_counter({counter, Name});
create_stat_folsom({histogram, Name}) ->
    folsom_metrics:new_histogram({histogram, Name}, slide, ?FLUSH_INTERVAL_MS),
    folsom_metrics:new_counter({units, Name}).

%% @doc Create the stats directory and setups the output file handles for dumping 
%% periodic CSV of histogram results.
init_histogram_files(Stats) -> 
    init_histogram_files(Stats, false).
init_histogram_files(Stats, NewDir) ->
    TestDir = get_stats_dir(?CURRENT_DIR),
    case (not filelib:is_dir(TestDir)) orelse NewDir of
        true -> create_new_dir();
        false -> ok
    end,
    ?PRINT(Stats),
    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, Name}, histogram_csv_file(Name,TestDir)) || {histogram, Name} <- Stats].

%% @doc Setups a histogram file for a stat.
histogram_csv_file(Label, Dir) ->
    Fname = normalize_label(Label) ++ "_hist.csv",
    Fname2 = filename:join([Dir, Fname]),
    {ok, F} = file:open(Fname2, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
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

