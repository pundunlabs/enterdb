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
    supervisor:start_link({local, enterdb_it_sup},
                          ?MODULE, [leveldb_it]);
start_link(Type) ->
    error_logger:error_msg("Enterdb backend type: ~p not suported yet.~n",
                           [Type]),
    {error, "not_supported"}.

init([rocksdb]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
	  [
	   #{id => enterdb_rdb_worker,
	     start => {enterdb_rdb_worker, start_link, []},
             restart => transient,
	     shutdown => 2000,
	     type => worker,
	      modules => [enterdb_rdb_worker]}
	  ]
	 }};
init([leveldb]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
          [
	   #{id => enterdb_ldb_worker,
	     start => {enterdb_ldb_worker, start_link, []},
	     restart => transient,
	     shutdown => 2000,
	     type => worker,
	     modules => [enterdb_ldb_worker]}
	  ]
	 }};
init([leveldb_wrp]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
          [#{id => enterdb_ldb_wrp,
	     start => {enterdb_ldb_wrp, start_link, []},
             restart => transient,
	     shutdown => 2000,
	     type => worker,
	     modules => [enterdb_ldb_wrp]}
	  ]
	 }};
init([leveldb_tda]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
          [
	   #{id => enterdb_ldb_tda,
	     start => {enterdb_ldb_tda, start_link, []},
             restart => transient,
	     shutdown => 2000,
	     type => worker,
	     modules => [enterdb_ldb_tda]}
	  ]
	 }};
init([leveldb_it]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
          [#{id => enterdb_it_worker,
	     start => {enterdb_it_worker, start_link, []},
	     restart => temporary,
	     shutdown => 2000,
	     type => worker,
	     modules => [enterdb_it_worker]}
	  ]
	 }}.
