-module(enterdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb.hrl").
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
-spec open_all_tables() ->
    ok | {error, Reason :: term()}.
open_all_tables() ->
    {ok, UserTabList} = open_system_tables(),
    enterdb_lib:open_shards(UserTabList).

-spec open_system_tables() ->
    {ok, UserTabList :: [string()]} | {error, Reason :: term()}.
open_system_tables() ->
    ?debug("Opening system tables..", []),
    Fun =
	fun(#enterdb_stab{shard = Shard, map = Map}, {S,U}) ->
	    case maps:get(system_table, Map, false) of
		true ->	{[Shard | S], U};
		false ->{S, [Shard | U]}
	    end
	end,
    case enterdb_db:transaction(fun() -> mnesia:foldl(Fun, {[],[]}, enterdb_stab) end) of
	{atomic, {SysTabList, UserTabList}} ->
	    enterdb_lib:open_shards(SysTabList),
	    {ok, UserTabList};
	{error, Reason} ->
	    {error, Reason}
    end.

-spec ensure_directories() ->
    ok.
ensure_directories() ->
    List = [ enterdb_lib:get_path(D) || D <- [db_path, wal_path, backup_path, checkpoint_path]],
    [ ok = filelib:ensure_dir(P) || P <- List, P =/= undefined],
    ok.
