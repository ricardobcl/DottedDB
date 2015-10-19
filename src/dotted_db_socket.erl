-module(dotted_db_socket).
-behaviour(gen_server).
-behaviour(ranch_protocol).

-include("dotted_db.hrl").

%% API.
-export([start_link/4]).

%% gen_server.
-export([init/1]).
-export([init/4]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TIMEOUT, 0).

-record(state, {socket, transport, client, options}).

%% API.

start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%% gen_server.

%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute.
init([]) -> {ok, undefined}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    {ok, Client} = dotted_db:new_client(node()),
    gen_server:enter_loop(?MODULE, [],
        #state{socket=Socket, transport=Transport, client=Client, options=[]}).

handle_info({tcp, Socket, BinData}, State=#state{
        socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),
    {ok, Data} = msgpack:unpack(BinData),
    State1 = commands(Data, State),
    {noreply, State1};
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};
handle_info({tcp_error, _, Reason}, State) ->
    {stop, Reason, State};
handle_info(timeout, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {stop, normal, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal.

commands(D=[<<"GET">>, Bucket, Key], S) ->
    lager:debug("GET Msg:~p",[D]),
    Response = case (S#state.client):get_at_node({Bucket, Key}) of
        {not_found, _Context} ->
            [<<"OK">>, {[]}];
        {ok, {Values, _Ctx}} ->
            [<<"OK">>, merge_to_map(Values)];
        {error, _Reason} ->
            [<<"ERROR">>, {[]}]
    end,
    lager:debug("GET Res:~p",[Response]),
    send(S, Response);

commands(D=[<<"PUT">>, Bucket, Key, Value], S) ->
    lager:debug("PUT Msg:~p",[D]),
    Response = case (S#state.client):new_at_node({Bucket, Key}, Value, S#state.options) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("PUT Res:~p",[Response]),
    send(S, Response);

commands(D=[<<"UPDATE">>, Bucket, Key, Value], S) ->
    lager:debug("UPDATE Msg:~p",[D]),
    Context = case (S#state.client):get_at_node({Bucket, Key}) of
        {ok, {_Values, Ctx}} -> 
            Ctx;
        {not_found, Ctx} -> 
            Ctx
    end,
    Response = case (S#state.client):put_at_node({Bucket, Key}, Value, Context, S#state.options) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("UPDATE Res:~p",[Response]),
    send(S, Response);

commands(D=[<<"DELETE">>, Bucket, Key], S) ->
    lager:debug("DELETE Msg:~p",[D]),
    Context = case (S#state.client):get_at_node({Bucket, Key}) of
        {ok, {_Values, Ctx}} -> 
            Ctx;
        {not_found, Ctx} -> 
            Ctx
    end,
    Response = case (S#state.client):delete_at_node({Bucket, Key}, Context, S#state.options) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("DELETE Res:~p",[Response]),
    send(S, Response);

commands(D=[<<"OPTIONS">>, Sync, ReplicationFailure, NodeFailure], S) ->
    lager:info("OPTIONS Msg:~p",[D]),
    %% set new sync interval
    dotted_db_sync_manager:set_sync_interval(Sync),
    %% set new replication message failure rate 0 <= rate <= 1
    State1 = S#state{options=[{?REPLICATION_FAIL_RATIO, ReplicationFailure}]},
    %% set new kill node rate
    dotted_db_sync_manager:set_kill_node_interval(NodeFailure),
    Response =  [<<"OK">>],
    lager:debug("OPTIONS Res:~p",[Response]),
    send(State1, Response);

commands(<<"quit\r\n">>, S) ->
    self() ! timeout,
    S;

commands(Data, S) ->
    lager:info("Unknown Msg:~p",[Data]),
    S.

merge_to_map([A]) -> A;
merge_to_map([A|[B|Tail]]) ->
    {LA} = A,
    {LB} = B,
    AB = {LA++LB},
    merge_to_map([AB|Tail]).

send(State, Response) ->
    (State#state.transport):send(State#state.socket, msgpack:pack(Response)),
    State.
