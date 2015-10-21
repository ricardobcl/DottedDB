-module(dotted_db_stats).

-behaviour(gen_server).


%% API for gen_server
-export([     start_link/0
            , start_link/1
            , add_stats/1
            , notify/2
            , start/0
            , stop/0
            , new_dir/0
        ]).

%% API for ETS
-export([   sync_complete/4,
            update_key_meta/6,
            compute_local_info/0,
            compute_vnode_info/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("dotted_db.hrl").

-define(FLUSH_INTERVAL, 3). % 3 seconds
-define(WARN_INTERVAL, 1000). % Warn once a second
-define(CURRENT_DIR, "current").
-define(ETS, ets_dotted_db_entropy).

-record(state, { 
        stats              = [],
        start_time         = os:timestamp(),
        last_write_time    = os:timestamp(),
        flush_interval     = ?FLUSH_INTERVAL*1000,
        timer              = undefined,
        active             = false,
        last_warn          = {0,0,0}
}).

-record(sync_stat, {
        last, 
        min, 
        max, 
        count, 
        sum, 
        fp, 
        tp, 
        total, 
        fp_size, 
        tp_size
}).
-type sync_stat()  :: #sync_stat{}.

-type sync_stats() :: {  Last   :: pos_integer(),
                         Min    :: pos_integer(),
                         Max    :: pos_integer(),
                         Mean   :: pos_integer(),
                         Sum    :: pos_integer(),
                        {FP     :: non_neg_integer(),   %% False Positives
                         TP     :: non_neg_integer(),   %% True Positives
                         Total  :: non_neg_integer(),   %% Total Transfers
                         FPS    :: non_neg_integer(),   %% False Positives Size
                         TPS    :: non_neg_integer()}}. %% True Positives Size

-record(vnode_stat, {
        meta_full       = 0,
        meta_strip      = 0,
        meta_full_len   = 0,
        meta_strip_len  = 0,
        meta_deleted    = 0,
        meta_count      = 0,
        nlc_size        = 0,
        nlc_count       = 0,
        kl_len          = 0,
        kl_size         = 0,
        vv_len          = 0,
        vv_size         = 0
}).
-type vnode_stat()  :: #vnode_stat{}.


-record(index_info, {
    sync  = undefined :: undefined | sync_stat(),
    vnode = undefined :: undefined |vnode_stat()
}).
-type index_info() :: #index_info{}.


%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    start_link([]).

start_link(Stats) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Stats], []).

%% @doc Dynamically adds a list of stats {Type, Name}
add_stats(NewStats) ->
    ?PRINT(NewStats),
    gen_server:call(?MODULE, {add_stats, NewStats}).

notify(Name, 0.0) -> notify(Name, 0);
notify(Name, Value) ->
    gen_server:cast(?MODULE, {notify, Name, Value}).

%% @doc Starts the timer that flush the data to disk.
start() ->
    gen_server:call(?MODULE, start).

%% @doc Stops the timer that flush the data to disk.
stop() ->
    gen_server:call(?MODULE, stop).

new_dir() ->
    gen_server:call(?MODULE, new_dir).

-spec compute_local_info() -> [{index(), sync_stats()}].
compute_local_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices    = riak_core_ring:my_indices(Ring),
    [{Index, get_sync_stat(Info#index_info.sync)} || 
        {{_, Index}, Info} <- all_index_info(), lists:member(Index, Indices)].

% -spec compute_vnode_info() -> [{index(), vnode_stats()}].
compute_vnode_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices    = riak_core_ring:my_indices(Ring),
    [{Index, get_vnode_stat(Info#index_info.vnode)} || 
        {{_, Index}, Info} <- all_index_info(), lists:member(Index, Indices)].

%% @doc Store information about a just-completed AAE exchange
sync_complete(Index, Repaired, Sent, {PayloadSize, MetaSize}) ->
    update_ets_info({index, Index}, {sync_complete, Repaired, Sent, {PayloadSize, MetaSize}}).

update_key_meta(_, 0, _, _, _, _) -> ok;
update_key_meta(Index, Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen) ->
    update_ets_info({index, Index}, {update_key_meta, Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen}).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([Stats]) ->
    %% Create the ETS table for (cumulative?) stats
    _ = create_table(),
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, normal),

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
    _ = init_histogram_files(Stats, true),
    %% Register each new stat with folsom.
    _ = [create_stat_folsom(Stat) || Stat <- Stats],

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
    _ = process_stats(Now, State),
    {reply, ok, State#state{    last_write_time = Now,
                                timer = undefined,
                                active = false}};

handle_call({add_stats, NewStats}, _From, State = #state{stats = CurrentStats}) ->
    ?PRINT("add_stats"),
    ?PRINT(NewStats),
    %% Create the stats directory and setups the output file handles for dumping
    %% periodic CSV of histogram results.
    _ = init_histogram_files(NewStats),
    %% Register each new stat with folsom.
    _ = [create_stat_folsom(Stat) || Stat <- NewStats],
    {reply, ok, State#state{stats = CurrentStats ++ NewStats}};

handle_call(new_dir, _From, State) ->
    %% Create a new folder for stats and point ?CURRENT_DIR to it.
    _ = init_histogram_files(State#state.stats, true),
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
    ok = folsom_metrics:notify({histogram, Name}, Value),
    ok = folsom_metrics:notify({units, Name}, {inc, 1}),
    {noreply, NewState};

handle_cast({notify, {counter, Name}, Value}, State = #state{active = true}) ->
    ok = folsom_metrics:notify({counter, Name}, {inc, Value}),
    {noreply, State}.


handle_info(flush, State) ->
    consume_flush_msgs(),
    Now = os:timestamp(),
    _ = process_stats(Now, State),
    {noreply, State#state { last_write_time = Now }}.

terminate(_Reason, State) ->
    %% Do the final stats report
    _ = process_stats(os:timestamp(), State),
    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% ====================================================================
%% Internal functions
%% ====================================================================

%%%%%%%%%
%% ETS
%%%%%%%%%
create_table() ->
    (ets:info(?ETS) /= undefined) orelse
        ets:new(?ETS, [named_table, public, set, {write_concurrency, true}]).

%% Utility function to get stored information for a given index,
%% invoke `handle_ets_info` to update the information, and then
%% store the new info back into the ETS table.
-spec update_ets_info({atom(), index()}, term()) -> ok.
update_ets_info(Key, Cmd) ->
    Info = case ets:lookup(?ETS, {index, Key}) of
               [] ->
                   #index_info{};
               [{_, I}] ->
                   I
           end,
    Info2 = handle_ets_info(Cmd, Info),
    ets:insert(?ETS, {{index, Key}, Info2}),
    ok.

%% Update provided index info based on request.
-spec handle_ets_info(term(), index_info()) -> index_info().
handle_ets_info({sync_complete, Repaired, Sent, {PayloadSize, MetaSize}}, Info) ->
    Sync = update_sync_complete(Repaired, Sent, {PayloadSize, MetaSize}, Info#index_info.sync),
    Info#index_info{sync=Sync};

handle_ets_info({update_key_meta, Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen}, Info) ->
    Vnode = update_key_meta_info(Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen, Info#index_info.vnode),
    Info#index_info{vnode=Vnode}.


update_key_meta_info(Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen, undefined) ->
    update_key_meta_info(Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen, #vnode_stat{});
update_key_meta_info(Count, MetaFull, MetaStrip, MetaFullLen, MetaStripLen, 
                            Vnode=#vnode_stat{ meta_full=MF, 
                                               meta_strip=MS, 
                                               meta_count=MC,
                                               meta_full_len=MFL,
                                               meta_strip_len=MSL}) ->
    Vnode#vnode_stat{   meta_full       = MetaFull + MF,
                        meta_strip      = MetaStrip + MS,
                        meta_full_len   = MetaFullLen + MFL,
                        meta_strip_len  = MetaStripLen + MSL,
                        meta_count      = MC + Count}.

get_vnode_stat(undefined) ->
    get_vnode_stat(#vnode_stat{});
get_vnode_stat(#vnode_stat{     meta_full=MF, 
                                meta_strip=MS, 
                                meta_count=MC,
                                meta_full_len=MFL,
                                meta_strip_len=MSL}) ->
    Savings    = 1 - (MS / max(1, MF)),
    SavingsL   = 1 - (MSL / max(1, MFL)),
    MeanF      = MF  / max(1, MC),
    MeanS      = MS  / max(1, MC),
    MeanFL     = MFL / max(1, MC),
    MeanSL     = MSL / max(1, MC),
    {MC, MF, MS, Savings, MeanF, MeanS, SavingsL, MeanFL, MeanSL}.





update_sync_complete(Repaired, Sent, {PayloadSize, MetaSize}, undefined) ->
    S = init_sync_stat(undefined),
    update_sync_complete(Repaired, Sent, {PayloadSize, MetaSize}, S);
update_sync_complete(Repaired, Sent, {PayloadSize, MetaSize}, S=#sync_stat{max=Max, min=Min, 
                            sum=Sum, count=_Cnt, total=Total, tp=TP, fp=FP, tp_size=TPSize, fp_size=FPSize}) -> 
    S#sync_stat{     last    = Repaired,
                     max     = erlang:max(Repaired, Max),
                     min     = erlang:min(Repaired, Min),
                     sum     = Sum+Repaired,
                     total   = Total + Sent,
                     tp      = TP + Repaired,
                     fp      = FP + Sent,
                     tp_size = TPSize + PayloadSize,
                     fp_size = FPSize + MetaSize}.

init_sync_stat(undefined) ->
    #sync_stat{last=0, min=0, max=0, sum=0, count=0, fp=0, tp=0, total=0, fp_size=0, tp_size=0}.

get_sync_stat(undefined) ->
    get_sync_stat(init_sync_stat(undefined));
get_sync_stat(#sync_stat{last=Last, max=Max, min=Min, sum=Sum, count=_Cnt,
                        fp=FP, tp=TP, total=Total, tp_size=TPSize, fp_size=FPSize}) ->
    FPRate    = FP / max(1, (FP+TP)),%wrong
    TotalRate = TP / max(1, Total),
    Mean      = Sum div max(1, Total),
    {Last, Min, Max, Mean, Sum, {FP, TP, FPRate, Total, TotalRate, FPSize, TPSize}}.


%% Return a list of all stored index information.
-spec all_index_info() -> [{{atom(), index()}, index_info()}].
all_index_info() ->
    ets:select(?ETS, [{{{index, '$1'}, '$2'}, [], [{{'$1','$2'}}]}]).


%%%%%%%%%%%%%%%%
%% gen_server
%%%%%%%%%%%%%%%

process_stats(Now, State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = round(timer:now_diff(Now, State#state.start_time) / 1000000),
    Window  = round(timer:now_diff(Now, State#state.last_write_time) / 1000000),
    [begin
         OpAmount = save_histogram(Elapsed, Window, Stat),
         folsom_metrics:notify({units, Stat}, {dec, OpAmount})
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
            lager:debug("No data for op: ~p\n", [Op]),
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
% create_stat_folsom(Name, spiral) ->
%     folsom_metrics:new_spiral({spiral, Name});
create_stat_folsom({counter, Name}) ->
    ok = folsom_metrics:new_counter({counter, Name});
create_stat_folsom({histogram, Name}) ->
    ok = folsom_metrics:new_histogram({histogram, Name}, slide, ?FLUSH_INTERVAL),
    ok = folsom_metrics:new_counter({units, Name}).

%% @doc Create the stats directory and setups the output file handles for dumping
%% periodic CSV of histogram results.
init_histogram_files(Stats) ->
    init_histogram_files(Stats, false).
init_histogram_files(Stats, NewDir) ->
    TestDir = get_stats_dir(?CURRENT_DIR),
    _ = case (not filelib:is_dir(TestDir)) orelse NewDir of
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
