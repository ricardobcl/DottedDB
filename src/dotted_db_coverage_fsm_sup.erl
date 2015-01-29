%% @doc Supervise the dotted_db_coverage FSM.
-module(dotted_db_coverage_fsm_sup).
-behavior(supervisor).

-export([start_link/0, 
         start_coverage_fsm/1]).
-export([init/1]).

start_coverage_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    CoverageFSM = {undefined,
                  {dotted_db_coverage_fsm, start_link, []},
                  temporary, 5000, worker, [dotted_db_coverage_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [CoverageFSM]}}.
