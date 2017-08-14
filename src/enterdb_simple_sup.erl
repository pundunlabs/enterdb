%%%===================================================================
%% @copyright 2017 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(enterdb_simple_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(rocksdb) ->
    supervisor:start_link({local, enterdb_rdb_sup},
                          ?MODULE, [rocksdb]);
start_link(enterdb_it) ->
    supervisor:start_link({local, enterdb_it_sup},
                          ?MODULE, [enterdb_it]);
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
init([enterdb_it]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 5},
          [#{id => enterdb_it_worker,
	     start => {enterdb_it_worker, start_link, []},
	     restart => temporary,
	     shutdown => 2000,
	     type => worker,
	     modules => [enterdb_it_worker]}
	  ]
	 }}.
