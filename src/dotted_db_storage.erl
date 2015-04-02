-module(dotted_db_storage).

-include("dotted_db.hrl").
-include_lib("rkvs/include/rkvs.hrl").

-export([   open/1,
            open/2,
            close/1,
            destroy/1,
            get/2,
            put/3,
            delete/2,
            write_batch/2,
            fold/3,
            fold_keys/3,
            is_empty/1
         ]).

-type storage() :: #engine{}.

-export_type([storage/0]).


%% @doc open a storage, and by default use levelDB as backend.
-spec open(list()) -> {ok, storage()} | {error, any()}.
open(Name) ->
    open(Name, [{backend, rkvs_leveldb}]).

%% @doc open a storage, and pass options to the backend.
-spec open(list(), list()) -> {ok, storage()} | {error, any()}.
open(Name, [{backend, leveldb}]) ->
    rkvs:open(Name, [{backend, rkvs_leveldb}]);
open(Name, [{backend, ets}]) ->
    rkvs:open(Name, [{backend, rkvs_ets}]).

%% @doc close a storage
-spec close(storage()) -> ok | {error, any()}.
close(Engine) ->
    rkvs:close(Engine).

%% @doc close a storage and remove all the data
-spec destroy(storage()) -> ok | {error, any()}.
destroy(Engine) ->
    rkvs:destroy(Engine).

%% @doc get the value associated to the key
-spec get(storage(), key()) -> any() | {error, term()}.
get(Engine, Key) ->
    BKey = dotted_db_utils:encode_kv(Key),
    rkvs:get(Engine, BKey).

%% @doc store the value associated to the key.
-spec put(storage(), key(), value()) -> ok | {error, term()}.
put(Engine, Key, Value) ->
    BKey = dotted_db_utils:encode_kv(Key),
    rkvs:put(Engine, BKey, Value).

%% @doc delete the value associated to the key
-spec delete(storage(), key()) -> ok | {error, term()}.
delete(Engine, Key) ->
    BKey = dotted_db_utils:encode_kv(Key),
    rkvs:clear(Engine, BKey).

%% @doc do multiple operations on the backend.
-spec write_batch(storage(), multi_ops()) -> ok | {error, term()}.
write_batch(Engine, Ops) ->
    rkvs:write_batch(Engine, Ops).

%% @doc fold all keys with a function
-spec fold_keys(storage(), fun(), any()) -> any() | {error, term()}.
fold_keys(Engine, Fun, Acc0) ->
    rkvs:fold_keys(Engine, Fun, Acc0, []).

%% @doc fold all K/Vs with a function
-spec fold(storage(), function(), any()) -> any() | {error, term()}.
fold(Engine, Fun, Acc0) ->
    rkvs:fold(Engine, Fun, Acc0, []).

%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(storage()) -> boolean() | {error, term()}.
is_empty(Engine) ->
    rkvs:is_empty(Engine).

