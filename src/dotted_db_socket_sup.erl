%% @doc Supervise the dotted_db_socket server.
-module(dotted_db_socket_sup).
-behavior(supervisor).

%% API.
-export([start_link/0]).

%% supervisor.
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor.

init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

