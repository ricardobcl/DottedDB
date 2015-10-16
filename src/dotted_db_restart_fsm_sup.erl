%% @doc Supervise the restart FSM.
-module(dotted_db_restart_fsm_sup).
-behavior(supervisor).

-export([start_restart_fsm/1,
         start_link/0]).
-export([init/1]).

start_restart_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    RestartFsm = {undefined,
                {dotted_db_restart_fsm, start_link, []},
                temporary, 5000, worker, [dotted_db_restart_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [RestartFsm]}}.
