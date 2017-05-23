-module(enterdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb.hrl").
-include("enterdb_internal.hrl").
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
		    gb_reg:new(?TABLE_LOOKUP_STR),
		    Res = open_system_tables(),
		    ?debug("Open system tables -> ~p", [Res]),
		    {ok, Pid};
		{error, Reason} ->
		    {error, Reason}
	end
    end.

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Open internal database table shards.
%% @end
%%--------------------------------------------------------------------
-spec open_system_tables() ->
    ok | {error, Reason :: term()}.
open_system_tables() ->
    ?info("Opening system tables..", []),
    Fun =
	fun(#enterdb_table{name = Name, map = Map}, Acc) ->
	    case maps:get(system_table, Map, false) of
		true ->	[Name | Acc];
		false -> Acc
	    end
	end,
    case enterdb_db:transaction(fun() -> mnesia:foldl(Fun, [], enterdb_table) end) of
	{atomic, SysTabList} ->
	    [enterdb_lib:open_table(Tab, false) || Tab <- SysTabList],
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec ensure_directories() ->
    ok.
ensure_directories() ->
    List = [ enterdb_lib:get_path(D) || D <- [db_path, wal_path, backup_path, checkpoint_path, hh_path, restore_path]],
    [ ok = filelib:ensure_dir(P) || P <- List, P =/= undefined],
    ok.
