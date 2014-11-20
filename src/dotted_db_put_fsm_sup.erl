%% @doc Supervise the rts_write FSM.
-module(dotted_db_put_fsm_sup).
-behavior(supervisor).

-export([start_put_fsm/1,
         start_link/0]).
-export([init/1]).

start_put_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    WriteFsm = {undefined,
                {dotted_db_put_fsm, start_link, []},
                temporary, 5000, worker, [dotted_db_put_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [WriteFsm]}}.
