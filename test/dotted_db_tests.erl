-module(dotted_db_tests).
-include_lib("eunit/include/eunit.hrl").
-export([test/0]).


%%% Setups/teardowns
test() ->
    application:start(dotted_db_app),
    ?assertNot(undefined == whereis(dotted_db_sup)),
    application:stop(dotted_db_app).




