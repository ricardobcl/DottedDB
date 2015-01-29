-module(dotted_db_coverage_fsm).

-include_lib("dotted_db.hrl").

-export([start_link/4, start/2]).
-export([init/2, process_results/2, finish/2]).

-behaviour(riak_core_coverage_fsm).

-record(state, {
    req_id,
    from,
    request,
    results=[]}).

%% API

start(Request, Timeout) ->
    ReqId = dotted_db_utils:make_request_id(),
    {ok, _} = dotted_db_coverage_fsm_sup:start_fsm([ReqId, self(), Request, Timeout]),
    receive
        {ReqId, Val} -> Val
    end.

start_link(ReqId, From, Request, Timeout) ->
    riak_core_coverage_fsm:start_link(?MODULE, {pid, ReqId, From}, [ReqId, From, Request, Timeout]).


%% riak_core_coverage_fsm API

init(_, [ReqId, From, Request, Timeout]) ->
    State = #state{req_id=ReqId, from=From, request=Request},
    {Request, allup, 1, 1, dotted_db, dotted_db_vnode_master, Timeout, State}.

process_results({{_ReqId, {Partition, Node}}, Data}, State=#state{results=Results}) ->
    NewResults = [{Partition, Node, Data}|Results],
    {done, State#state{results=NewResults}}.

finish(clean, S=#state{req_id=ReqId, from=From, results=Results}) ->
    From ! {ReqId, ok, coverage, Results},
    {stop, normal, S};

finish({error, Reason}, S=#state{req_id=ReqId, from=From, results=Results}) ->
    lager:warning("Coverage query failed! Reason: ~p", [Reason]),
    From ! {ReqId, {partial, Reason, Results}},
    {stop, normal, S}.
