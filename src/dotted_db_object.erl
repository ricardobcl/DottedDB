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
            , strip2/2
            , fill/3
            , sync/2
            , add_to_node_clock/2
            , equal/2
            , equal_values/2
            , discard_values/2
            , add_value/3
            , get_values/1
            , get_context/1
        ]).

-record(object, {
    container   :: dcc(),
    lastFSMtime :: erlang:timestamp() | undefined
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

-spec get_fsm_time(object()) -> erlang:timestamp() | undefined.
get_fsm_time(Object) ->
    Object#object.lastFSMtime.

-spec set_fsm_time(erlang:timestamp() | undefined, object()) -> object().
set_fsm_time(undefined, Object) ->
    Object;
set_fsm_time(FSMtime, Object) ->
    Object#object{lastFSMtime = FSMtime}.

-spec strip(vv_matrix(), object()) -> object().
strip(WM, Object) ->
    MinWM = swc_watermark:min_all(WM, ?ENTRIES_WM),
    DCC = swc_kv:strip_wm(get_container(Object), MinWM),
    set_container(DCC, Object).

-spec strip2(vv(), object()) -> object().
strip2(MinWM, Object) ->
    DCC = swc_kv:strip_wm(get_container(Object), MinWM),
    set_container(DCC, Object).

-spec fill(key(), bvv(), object()) -> object().
fill(_Key, _NodeClock, Object) ->
    % no fill in this version
    Object.

-spec sync(object(), object()) -> object().
sync(O1, O2) ->
    DCC = swc_kv:sync(get_container(O1), get_container(O2)),
    case {get_fsm_time(O1), get_fsm_time(O2)} of
        {_, undefined}  -> set_container(DCC, O1);
        {undefined, _}  -> set_container(DCC, O2);
        {A, B} ->
            case A > B of
                true -> set_container(DCC, O1);
                false -> set_container(DCC, O2)
            end
    end.

-spec add_to_node_clock(bvv(), object()) -> bvv().
add_to_node_clock(NodeClock, Object) ->
    swc_kv:add(NodeClock, get_container(Object)).

-spec equal(object(), object()) -> boolean().
equal(O1, O2) ->
    get_container(O1) == get_container(O2).

-spec equal_values(object(), object()) -> boolean().
equal_values(O1, O2) ->
    {D1,_} = get_container(O1),
    {D2,_} = get_container(O2),
    D1 == D2.

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
