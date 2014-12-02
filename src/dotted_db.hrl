-include_lib("glc/include/glc.hrl").

-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(DEFAULT_TIMEOUT, 10000).
-define(N, 3).
-define(R, 2).
-define(W, 2).
-define(DEFAULT_BUCKET, <<"g">>).
-define(DELETE_OP, delete_op).
-define(WRITE_OP, write_op).

-type key()         :: id().
% index in the consistent hashing ring
-type index()       :: non_neg_integer().
% element of the consistent hashing ring
-type index_node()  :: {index(), node()}.

-type keylog()      :: {counter(), [key()]}.

-type multi_ops()   :: [{put, key(), value()}
                       |{delete, key(), value()}].

-record(stats_sync, {
        % the node initiating the sync process
        nodeA                   :: id(),
        % the node contacted by node a to synchronize
        nodeB                   :: id(),
        % starting time
        start_time              :: float(),
        % ending time
        ending_time             :: float(),
        % number of objects transferred from b to a
        b2a_number              :: non_neg_integer(),
        % transfer size sent from b to a
        b2a_size                :: non_neg_integer(),
        % transfer size sent from b to a if there was no causality stripping
        b2a_size_full           :: non_neg_integer(),
        % node b keylog length
        keylog_length_b         :: non_neg_integer(),
        % node b keylog size
        keylog_size_b           :: non_neg_integer(),
        % node b replicated VV size
        replicated_vv_size_b    :: non_neg_integer()
    }).


-record(stats_reqs, {
        % deleted obj size
        delete_size     :: non_neg_integer(),
        % transfer size sent from b to a
        db_size         :: non_neg_integer(),
        % transfer size sent from b to a if there was no causality stripping
        db_size_full    :: non_neg_integer(),
        % node clock size
        bvv_size        :: non_neg_integer(),
        % the keylog size
        keylog_size     :: non_neg_integer(),
        % the replicated VV size
        replicated_size :: non_neg_integer()
        % fast write vs normal write
        % TODO
    }).


-type stats_sync()  :: #stats_sync{}.
-type stats_reqs()  :: #stats_reqs{}.

