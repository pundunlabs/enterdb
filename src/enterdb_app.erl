-module(enterdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include_lib("gb_log/include/gb_log.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ok = ensure_directories(),
    case mnesia:wait_for_tables([enterdb_table, enterdb_stab], 20000) of
        {timeout, RemainingTabs} ->
	    {error, {not_exists, RemainingTabs}};
        ok ->
	    case enterdb_sup:start_link() of
		{ok, Pid} ->
		    Res = open_all_tables(),
		    ?debug("Open all tables.. ~p", [Res]),
		    {ok, Pid};
		{error, Reason} ->
		    {error, Reason}
	end
    end.

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Open existing database table shards.
%% @end
%%--------------------------------------------------------------------
-spec open_all_tables() -> ok | {error, Reason :: term()}.
open_all_tables() ->
    ?debug("Opening all tables..", []),
    case enterdb_db:transaction(fun() -> mnesia:all_keys(enterdb_stab) end) of
	{atomic, DBList} ->
	    enterdb_lib:open_shards(DBList);
	{error, Reason} ->
	    {error, Reason}
    end.

-spec ensure_directories() ->
    ok.
ensure_directories() ->
    List = [ enterdb_lib:get_path(D) || D <- [db_path, wal_path, backup_path, checkpoint_path]],
    [ ok = filelib:ensure_dir(P) || P <- List, P =/= undefined],
    ok.
