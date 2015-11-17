-module(dotted_db_object).

-include("dotted_db.hrl").

%% API
-export([     new/0
            , new/1
            , get_container/1
            , set_container/2
            , get_fsm_time/1
            , set_fsm_time/2
            , strip/2
            , fill/3
            , sync/2
            , add_to_node_clock/2
            , equal/2
            , discard_values/2
            , add_value/3
            , get_values/1
            , get_context/1
        ]).

-record(object, {
    container   :: dcc(),
    lastFSMtime :: erlang:timestamp()
}).

-opaque object() :: #object{}.

-export_type([object/0]).

%% API

-spec new() -> object().
new() ->
    #object{
        container   = swc_kv:new(),
        lastFSMtime = undefined
    }.

-spec new(dcc()) -> object().
new(DCC) ->
    #object{
        container   = DCC,
        lastFSMtime = undefined
    }.

-spec get_container(object()) -> dcc().
get_container(Object) ->
    Object#object.container.

-spec set_container(dcc(), object()) -> object().
set_container(DCC, Object) ->
    Object#object{container = DCC}.

-spec get_fsm_time(object()) -> erlang:timestamp().
get_fsm_time(Object) ->
    Object#object.lastFSMtime.

-spec set_fsm_time(erlang:timestamp(), object()) -> object().
set_fsm_time(undefined, Object) ->
    Object;
set_fsm_time(FSMtime, Object) ->
    Object#object{lastFSMtime = FSMtime}.

-spec strip(bvv(), object()) -> object().
strip(NodeClock, Object) ->
    DCC = swc_kv:strip(get_container(Object), NodeClock),
    set_container(DCC, Object).

-spec fill(key(), bvv(), object()) -> object().
fill(Key, NodeClock, Object) ->
    {D,VV} = get_container(Object),
    RNIndices = dotted_db_utils:replica_nodes_indices(Key),
    case ?REPLICATION_FACTOR == length(RNIndices) of
        true ->
            % only consider ids that belong to both the list of ids received and the NodeClock
            NodeVV = [{Id,N} || {Id={Index,_}, {N,_}} <- NodeClock, lists:member(Index, RNIndices)],
            DCC = {D, swc_vv:join(VV, NodeVV)},
            set_container(DCC, Object);
        false ->
            lager:error("fill clock: RF:~p RNind:~p for key:~p indices:~p",
                [?REPLICATION_FACTOR, length(RNIndices), Key, RNIndices]),
            % swc_kv:fill(LocalClock, NodeClock)
            ?REPLICATION_FACTOR = length(RNIndices)
    end.

-spec sync(object(), object()) -> object().
sync(O1, O2) ->
    DCC = swc_kv:sync(get_container(O1), get_container(O2)),
    case get_fsm_time(O1) of
        undefined   -> set_container(DCC, O2);
        _           -> set_container(DCC, O1)
    end.

-spec add_to_node_clock(bvv(), object()) -> bvv().
add_to_node_clock(NodeClock, Object) ->
    swc_kv:add(NodeClock, get_container(Object)).

-spec equal(object(), object()) -> boolean().
equal(O1, O2) ->
    get_container(O1) == get_container(O2).

-spec discard_values(vv(), object()) -> object().
discard_values(Context, Object) ->
    DCC = swc_kv:discard(get_container(Object), Context),
    set_container(DCC, Object).

-spec add_value(dot(), value(), object()) -> object().
add_value(Dot, Value, Object) ->
    DCC = swc_kv:add(get_container(Object), Dot, Value),
    set_container(DCC, Object).

-spec get_values(object()) -> [value()].
get_values(Object) ->
    swc_kv:values(get_container(Object)).

-spec get_context(object()) -> vv().
get_context(Object) ->
    swc_kv:context(get_container(Object)).
