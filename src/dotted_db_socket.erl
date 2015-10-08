-module(dotted_db_socket).
-behaviour(gen_server).
-behaviour(ranch_protocol).

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

-record(state, {socket, transport, client}).

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
        #state{socket=Socket, transport=Transport, client=Client}).

handle_info({tcp, Socket, BinData}, State=#state{
        socket=Socket, transport=Transport, client=Client}) ->
    Transport:setopts(Socket, [{active, once}]),
    {ok, Data} = msgpack:unpack(BinData),
    commands(Data, Socket, Transport, Client),
    {noreply, State};
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

commands(D=[<<"GET">>, Key], S, T, C) ->
    lager:debug("GET Msg:~p",[D]),
    Response = case C:get_at_node(Key) of
        {not_found, _Context} ->
            [<<"OK">>, {[]}];
        {ok, {Values, _Ctx}} ->
            [<<"OK">>, merge_to_map(Values)];
        {error, _Reason} ->
            [<<"ERROR">>, {[]}]
    end,
    lager:debug("GET Res:~p",[Response]),
    T:send(S, msgpack:pack(Response));
commands(D=[<<"PUT">>, Key, Value], S, T, C) ->
    lager:debug("PUT Msg:~p",[D]),
    Response = case C:new_at_node(Key, Value) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("PUT Res:~p",[Response]),
    T:send(S, msgpack:pack(Response));
commands(D=[<<"UPDATE">>, Key, Value], S, T, C) ->
    lager:debug("UPDATE Msg:~p",[D]),
    Context = case C:get_at_node(Key) of
        {ok, {_Values, Ctx}} -> 
            Ctx;
        {not_found, Ctx} -> 
            Ctx
    end,
    Response = case C:put_at_node(Key, Value, Context) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("UPDATE Res:~p",[Response]),
    T:send(S, msgpack:pack(Response));
commands(D=[<<"DELETE">>, Key], S, T, C) ->
    lager:debug("DELETE Msg:~p",[D]),
    Context = case C:get_at_node(Key) of
        {ok, {_Values, Ctx}} -> 
            Ctx;
        {not_found, Ctx} -> 
            Ctx
    end,
    Response = case C:delete_at_node(Key, Context) of
        ok ->
            [<<"OK">>];
        {error, _Reason} ->
            [<<"ERROR">>]
    end,
    lager:debug("DELETE Res:~p",[Response]),
    T:send(S, msgpack:pack(Response));
commands(<<"quit\r\n">>, _, _, _) ->
    self() ! timeout;
commands(Data, S, T, _C) ->
    lager:info("Unknown Msg:~p",[Data]),
    Response = [<<"Key">>, <<"Value">>],
    T:send(S, msgpack:pack(Response)).

merge_to_map([A]) -> A;
merge_to_map([A|[B|Tail]]) ->
    {LA} = A,
    {LB} = B,
    AB = {LA++LB},
    merge_to_map([AB|Tail]).