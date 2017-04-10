-module(enterdb_simple_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(rocksdb) ->
    supervisor:start_link({local, enterdb_rdb_sup},
                          ?MODULE, [rocksdb]);
start_link(leveldb) ->
    supervisor:start_link({local, enterdb_ldb_sup},
                          ?MODULE, [leveldb]);
start_link(leveldb_wrp) ->
    supervisor:start_link({local, enterdb_wrp_sup},
                          ?MODULE, [leveldb_wrp]);
start_link(leveldb_tda) ->
    supervisor:start_link({local, enterdb_tda_sup},
                          ?MODULE, [leveldb_tda]);
start_link(leveldb_it) ->
    supervisor:start_link({local, enterdb_lit_sup},
                          ?MODULE, [leveldb_it]);
start_link(Type) ->
    error_logger:error_msg("Enterdb backend type: ~p not suported yet.~n",
                           [Type]),
    {error, "not_supported"}.

init([rocksdb]) ->
    {ok, {{simple_one_for_one, 10, 5},
          [{enterdb_rdb_worker, {enterdb_rdb_worker, start_link, []},
            transient, 2000, worker, [enterdb_rdb_worker]}]}};
init([leveldb]) ->
    {ok, {{simple_one_for_one, 10, 5},
          [{enterdb_ldb_worker, {enterdb_ldb_worker, start_link, []},
            transient, 2000, worker, [enterdb_ldb_worker]}]}};
init([leveldb_wrp]) ->
    {ok, {{simple_one_for_one, 10, 5},
          [{enterdb_ldb_wrp, {enterdb_ldb_wrp, start_link, []},
            transient, 2000, worker, [enterdb_ldb_wrp]}]}};
init([leveldb_tda]) ->
    {ok, {{simple_one_for_one, 10, 5},
          [{enterdb_ldb_tda, {enterdb_ldb_tda, start_link, []},
            transient, 2000, worker, [enterdb_ldb_tda]}]}};
init([leveldb_it]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{enterdb_lit_worker, {enterdb_lit_worker, start_link, []},
            temporary, 2000, worker, [enterdb_lit_worker]}]}}.
