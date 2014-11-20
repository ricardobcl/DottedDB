%%%-------------------------------------------------------------------
%%%
%%% File:   vv.erl
%%%
%%% @title  Version Vector
%%% @author Ricardo Gonçalves <tome.wave@gmail.com>
%%%
%%% @doc  
%%% An Erlang implementation of a Version Vector
%%% @end  
%%%
%%%
%%%-------------------------------------------------------------------

-module(vv).
-author('Ricardo Gonçalves <tome.wave@gmail.com>').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("dotted_db.hrl").

-export([ new/0
        , ids/1
        , get/2
        , join/2
        , filter/2
        , add/2
        , min/1
        ]).

-export_type([vv/0]).

%% @doc Initializes a new empty version vector.
-spec new() -> vv().
new() -> orddict:new().

%% @doc Returns all the keys (ids) from a VV.
-spec ids(vv()) -> [id()].
ids(V) ->
    orddict:fetch_keys(V).

%% @doc Returns the counter associated with an id K. If the key is not present
%% in the VV, it returns 0.
-spec get(id(), vv()) -> {ok, counter()} | error.
get(K,V) ->
    case orddict:find(K,V) of
        error   -> 0;
        {ok, C} -> C
    end.

%% @doc Merges or joins two VVs, taking the maximum counter if an entry is
%% present in both VVs.
-spec join(vv(), vv()) -> vv().
join(A,B) ->
    FunMerge = fun (_Id, C1, C2) -> max(C1, C2) end,
    orddict:merge(FunMerge, A, B).

%% @doc It applies some boolean function F to all entries in the VV, removing
%% those that return False when F is used.
-spec filter(fun((id(), counter()) -> boolean()), vv()) -> vv().
filter(F,V) ->
    orddict:filter(F, V).

%% @doc Adds an entry {Id, Counter} to the VV, performing the maximum between
%% both counters, if the entry already exists.
-spec add(vv(), {id(), counter()}) -> vv().
add(VV, {Id, Counter}) ->
    Fun = fun (C) -> max(C, Counter) end,
    orddict:update(Id, Fun, Counter, VV).

%% @doc Returns the mininum counters from all the entries in the VV.
-spec min(vv()) -> counter().
min(VV) ->
    Keys = orddict:fetch_keys(VV),
    Values = [orddict:fetch(Key, VV) || Key <- Keys],
    lists:min(Values).
