-module(enterdb_simple_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(leveldb) ->
    supervisor:start_link({local, enterdb_ldb_sup},
                          ?MODULE, [leveldb]);
start_link(Backend) ->
    error_logger:error_msg("Enterdb backend: ~p not supoorted yet.~n",
                           [Backend]),
    {error, "not_supported"}.

init([leveldb]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{enterdb_ldb_worker, {enterdb_ldb_worker, start_link, []},
            temporary, 2000, worker, [enterdb_ldb_worker]}]}}.