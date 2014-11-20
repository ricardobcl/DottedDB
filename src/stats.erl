-module(stats).

-export([     new_sync/0
            , start_sync/1
            , nodes/3
            , sync_req/2
            , end_sync/1
            , pp/1
        ]).

-include_lib("dotted_db.hrl").

new_sync() ->
    #stats_sync{
        nodeA                   = undefined,
        nodeB                   = undefined,
        start_time              = 0.0,
        ending_time             = 0.0,
        b2a_number              = 0,
        b2a_size                = 0,
        b2a_size_full           = 0,
        keylog_length_b         = 0,
        keylog_size_b           = 0,
        replicated_vv_size_b    = 0
    }.


start_sync(S=#stats_sync{}) ->
    S#stats_sync{start_time = get_time_sec()}.

nodes(S=#stats_sync{}, NodeA, NodeB) ->
     S#stats_sync{nodeA = NodeA, nodeB = NodeB}.

sync_req(S=#stats_sync{}, {VV,KLS,KLL,OL,SOS,FOS}) ->
    S#stats_sync{
        b2a_number              = OL,
        b2a_size                = SOS,
        b2a_size_full           = FOS,
        keylog_length_b         = KLL,
        keylog_size_b           = KLS,
        replicated_vv_size_b    = VV
    }.

end_sync(S=#stats_sync{}) ->
    S#stats_sync{ending_time = get_time_sec()}.

% pretty print
pp(#stats_sync{
        nodeA                   = A,
        nodeB                   = B,
        start_time              = ST,
        ending_time             = ET,
        b2a_number              = OL,
        b2a_size                = SOS,
        b2a_size_full           = FOS,
        keylog_length_b         = KLL,
        keylog_size_b           = KLS,
        replicated_vv_size_b    = VV }) ->
    io:format("Sync from node:~p to node:~p\n",[A,B]),
    io:format("\t Duration (micro):  \t ~p\n",[ET-ST]),
    io:format("\t Objects Transfers: \t ~p\n",[OL]),
    io:format("\t Objects Size:      \t ~p\n",[SOS]),
    io:format("\t Objects Size Full: \t ~p\n",[FOS]),
    io:format("\t KeyLog Length:     \t ~p\n",[KLL]),
    io:format("\t KeyLog Size:       \t ~p\n",[KLS]),
    io:format("\t Replicated VV Size:\t ~p\n",[VV]).


%% Internal Functions

-spec get_time_sec() -> float().
get_time_sec() ->
    {Mega,Sec,Micro} = os:timestamp(),
    % (Mega * 1000000) + Sec + (Micro / 1000000).
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.
