%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%%
%%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

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

    EdbServer	    = {enterdb_server, {enterdb_server, start_link, []},
			permanent, 20000, worker, [enterdb_server]},
    EdbMemMgrServer = {enterdb_mem_wrp_mgr, {enterdb_mem_wrp_mgr, start_link, []},
			permanent, 2000, worker, [enterdb_mem_wrp_mgr]},
    EdbLDBSup	    = {enterdb_ldb_sup,
			{enterdb_simple_sup, start_link,[leveldb]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbLITSup	    = {enterdb_lit_sup,
			{enterdb_simple_sup, start_link,[leveldb_it]},
			permanent, infinity, supervisor,[enterdb_simple_sup]},
    EdbNS	    = {enterdb_ns, {enterdb_ns, start_link, []},
			permanent, 20000, worker, [enterdb_ns]},
    {ok, {SupFlags, [EdbNS, EdbLITSup, EdbLDBSup, EdbMemMgrServer, EdbServer]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
