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
    SupFlags = #{strategy => one_for_one,
		 intensity => 4,
		 period => 3600},

    EdbMemMgrServer = #{id => enterdb_mem_wrp_mgr,
			start => {enterdb_mem_wrp_mgr, start_link, []},
			restart => permanent,
			shutdown => 20000,
			type => worker,
			modules => [enterdb_mem_wrp_mgr]},
    EdbRdbSup	    = #{id => enterdb_rdb_sup,
			start => {enterdb_simple_sup, start_link,[rocksdb]},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [enterdb_simple_sup]},
    EdbLdbSup	    = #{id => enterdb_ldb_sup,
			start => {enterdb_simple_sup, start_link,[leveldb]},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [enterdb_simple_sup]},
    EdbLdbWrpSup    = #{id => enterdb_wrp_sup,
			start => {enterdb_simple_sup, start_link,[leveldb_wrp]},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [enterdb_simple_sup]},
    EdbLdbTdaSup    = #{id => enterdb_tda_sup,
			start => {enterdb_simple_sup, start_link,[leveldb_tda]},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [enterdb_simple_sup]},
    EdbLitSup	    = #{id => enterdb_it_sup,
			start => {enterdb_simple_sup, start_link,[leveldb_it]},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [enterdb_simple_sup]},
    EdbNS	    = #{id => enterdb_ns,
			start => {enterdb_ns, start_link, []},
			restart => permanent,
			shutdown => 20000,
			type => worker,
			modules => [enterdb_ns]},
    EdbRS	    = #{id => enterdb_rs,
			start => {enterdb_rs, start_link, []},
			restart => permanent,
			shutdown => 20000,
			type => worker,
			modules => [enterdb_rs]},
    EdbPTS	    = #{id => enterdb_pts,
			start => {enterdb_pts, start_link, []},
			restart => permanent,
			shutdown => 20000,
			type => worker,
			modules => [enterdb_pts]},
    EdbIndexUpdate  = #{id => enterdb_index_update,
			start => {enterdb_index_update, start_link, []},
			restart => permanent,
			shutdown => 20000,
			type => worker,
			modules => [enterdb_index_update]},

    {ok, {SupFlags, [EdbNS, EdbRS, EdbPTS, EdbLitSup,
		     EdbLdbWrpSup, EdbLdbTdaSup, EdbRdbSup, EdbLdbSup,
		     EdbMemMgrServer, EdbIndexUpdate]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
