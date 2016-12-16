%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2016 Pundun Labs AB
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
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(enterdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-include_lib("gb_log/include/gb_log.hrl").
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% 
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
		      ignore |
		      {error, Error :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 4,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    EdbMemMgrServer = {enterdb_mem_wrp_mgr, {enterdb_mem_wrp_mgr, start_link, []},
			permanent, 2000, worker, [enterdb_mem_wrp_mgr]},
    EdbLdbSup	    = {enterdb_ldb_sup,
			{enterdb_simple_sup, start_link,[leveldb]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbLdbWrpSup    = {enterdb_wrp_sup,
			{enterdb_simple_sup, start_link,[leveldb_wrp]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbLdbTdaSup    = {enterdb_tda_sup,
			{enterdb_simple_sup, start_link,[leveldb_tda]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbLitSup	    = {enterdb_lit_sup,
			{enterdb_simple_sup, start_link,[leveldb_it]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbNS	    = {enterdb_ns, {enterdb_ns, start_link, []},
			permanent, 20000, worker, [enterdb_ns]},
    EdbRS	    = {enterdb_rs, {enterdb_rs, start_link, []},
			permanent, 20000, worker, [enterdb_rs]},

    {ok, {SupFlags, [EdbNS, EdbRS, EdbLitSup,
		     EdbLdbWrpSup, EdbLdbTdaSup, EdbLdbSup,
		     EdbMemMgrServer]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
