-module(dotted_db_storage).
-behaviour(gen_server).

-include("dotted_db.hrl").
-include_lib("rkvs/include/rkvs.hrl").

-export([
            open/1,
            open/2,
            open/3,
            close/1,
            destroy/1,
            get/2,
            put/3,
            delete/2,
            write_batch/2,
            fold/3,
            fold_keys/3,
            is_empty/1,
            drop/1
         ]).


% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-type storage() :: pid().
% -type backend() :: bitcask | ets | leveldb.

-export_type([storage/0]).


-record(state, {engine :: engine()}).

%% @doc Start the server and open the storage, which by default is an ETS backend.
open(Name) ->
    open(Name, {backend, ets}, []).

open(Name, Backend) ->
    open(Name, Backend, []).

open(Name, {backend, Type}, Options) ->
    case gen_server:start_link({local, ?MODULE}, ?MODULE, {Name, Type, [{value_encoding, term} | Options]}, []) of
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {ok, Pid} ->
            {ok, Pid}
    end.


%% @doc close a storage
-spec close(storage()) -> ok | {error, any()}.
close(Storage) ->
    gen_server:cast(Storage, close).


%% @doc close a storage and remove all the data
-spec destroy(storage()) -> ok | {error, any()}.
destroy(Storage) ->
    gen_server:cast(Storage, destroy).

%% @doc get the value associated to the key
-spec get(storage(), key()) -> any() | {error, term()}.
get(Storage, Key) ->
    gen_server:call(Storage, {get, Key}).

%% @doc store the value associated to the key.
-spec put(storage(), key(), value()) -> ok | {error, term()}.
put(Storage, Key, Value) ->
    gen_server:cast(Storage, {put, Key, Value}).

%% @doc delete the value associated to the key
-spec delete(storage(), key()) -> ok | {error, term()}.
delete(Storage, Key) ->
    gen_server:cast(Storage, {delete, Key}).

%% @doc do multiple operations on the backend.
-spec write_batch(storage(), multi_ops()) -> ok | {error, term()}.
write_batch(Storage, Ops) ->
    gen_server:cast(Storage, {put_batch, Ops}).

%% @doc fold all keys with a function
-spec fold_keys(storage(), fun(), any()) -> any() | {error, term()}.
fold_keys(Storage, Fun, Acc) ->
    gen_server:call(Storage, {fold_keys, Fun, Acc}).

%% @doc fold all K/Vs with a function
-spec fold(storage(), function(), any()) -> any() | {error, term()}.
fold(Storage, Fun, Acc) ->
    gen_server:call(Storage, {fold, Fun, Acc}).

%% @doc Returns true if this backend has no values; otherwise returns false.
-spec is_empty(storage()) -> boolean() | {error, term()}.
is_empty(Storage) ->
    gen_server:call(Storage, is_empty).

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(storage()) -> {ok, storage()} | {error, term(), storage()}.
drop(Storage) ->
    gen_server:call(Storage, drop).




%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Name, ets, Options}) ->
    process_flag(trap_exit, true),
    {ok, Engine} = rkvs:open(Name, [{backend, rkvs_ets}|Options]),
    {ok, #state{engine = Engine}};
init({Name, bitcask, Options}) ->
    process_flag(trap_exit, true),
    {ok, Engine} = rkvs:open(Name, [{backend, rkvs_bitcask}|Options]),
    {ok, #state{engine = Engine}};
init({Name, leveldb, Options}) ->
    process_flag(trap_exit, true),
    {ok, Engine} = try_open_level_db(Name, 5, undefined, Options),
    {ok, #state{engine = Engine}}.


handle_call({get, Key}, _From, State) ->
    BKey = dotted_db_utils:encode_kv(Key),
    Value = rkvs:get(State#state.engine, BKey),
    {reply, Value, State};

handle_call({fold_keys, Fun, Acc}, _From, State) ->
    Res = rkvs:fold_keys(State#state.engine, Fun, Acc, []),
    {reply, Res, State};

handle_call({fold, Fun, Acc}, _From, State) ->
    Res = rkvs:fold(State#state.engine, Fun, Acc, []),
    {reply, Res, State};

handle_call(is_empty, _From, State) ->
    Res = rkvs:is_empty(State#state.engine),
    {reply, Res, State};

handle_call(drop, _From, State) ->
    Res = drop(State#state.engine, 2, undefined),
    {reply, Res, State}.


handle_cast({put, Key, Value}, State) ->
    BKey = dotted_db_utils:encode_kv(Key),
    rkvs:put(State#state.engine, BKey, Value),
    {noreply, State};

handle_cast({delete, Key}, State) ->
    BKey = dotted_db_utils:encode_kv(Key),
    rkvs:clear(State#state.engine, BKey),
    {noreply, State};

handle_cast({put_batch, Ops}, State) ->
    Fun = fun
            ({put, K, V}) -> {put, dotted_db_utils:encode_kv(K), V};
            ({delete, K}) -> {delete, dotted_db_utils:encode_kv(K)}
        end,
    rkvs:write_batch(State#state.engine, lists:map(Fun, Ops)),
    {noreply, State};

handle_cast(destroy, State) ->
    rkvs:destroy(State#state.engine),
    {stop, normal, State};

handle_cast(close, State) ->
    Ref = (State#state.engine)#engine.ref,
    case Ref of
        undefined ->
            ok;
        <<>> ->
            ok;
        _ ->
            rkvs:close(State#state.engine)
    end,
    {stop, normal, State}.


%% Informative calls
% {noreply,NewState}
% {noreply,NewState,Timeout}
% {noreply,NewState,hibernate}
% {stop,Reason,NewState}
handle_info(_Message, _Server) ->
    io:format("Generic info handler: '~p' '~p'~n",[_Message, _Server]),
    {noreply, _Server}.


terminate(_Reason, State) ->
    Ref = (State#state.engine)#engine.ref,
    case Ref of
        undefined ->
            ok;
        <<>> ->
            ok;
        _ ->
            rkvs:close(State#state.engine)
    end.

code_change(_OldVersion, _Server, _Extra) ->
    {ok, _Server}.

%%====================================================================
%% Helper Functions
%%====================================================================

try_open_level_db(_Name, 0, LastError, _Options) ->
    {error, LastError};
try_open_level_db(Name, RetriesLeft, _, Options) ->
    case rkvs:open(Name, [{backend, rkvs_leveldb}|Options]) of
        {ok, Engine} ->
            {ok, Engine};
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = 2000,
                    lager:warning("Leveldb Open backend retrying ~p in ~p ms after error ~s\n",
                                [Name, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    try_open_level_db(Name, RetriesLeft - 1, Reason, Options);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


drop(Engine, 0, LastError) ->
    % os:cmd("rm -rf " ++ Engine#engine.name),
    {error, LastError, Engine};
drop(Engine, RetriesLeft, _) ->
    ok = close(Engine),
    % Engine2 = Engine#engine{options=[{db_opts,{create_if_missing, false}}]},
    case rkvs:destroy(Engine) of
        ok ->
            % {ok, Engine#engine{ref = undefined}};
            {ok, Engine};
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {error_db_destroy, DestroyErr}=Reason} ->
            case lists:prefix("IO error: lock ", DestroyErr) of
                true ->
                    SleepFor = 2000,
                    % lager:warning("Leveldb destroy backend retrying {~p,~p} in ~p ms after error ~s\n",
                                % [Engine#engine.name, node(), SleepFor, DestroyErr]),
                    timer:sleep(SleepFor),
                    drop(Engine, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason, Engine}
            end;
        {error, Reason} ->
            {error, Reason, Engine}
    end.


