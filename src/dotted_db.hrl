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
