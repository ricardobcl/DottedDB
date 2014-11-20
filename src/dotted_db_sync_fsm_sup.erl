%% @doc Supervise the rts_write FSM.
-module(dotted_db_sync_fsm_sup).
-behavior(supervisor).

-export([start_sync_fsm/1,
         start_link/0]).
-export([init/1]).

start_sync_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SyncFsm = {undefined,
                {dotted_db_sync_fsm, start_link, []},
                temporary, 5000, worker, [dotted_db_sync_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [SyncFsm]}}.
