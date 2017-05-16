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
		    Res = open_internal_tables(),
		    ?debug("Open internal tables -> ~p", [Res]),
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
-spec open_internal_tables() -> ok | {error, Reason :: term()}.
open_internal_tables() ->
    ?info("Opening internal tables first"),
    Tabs = ["gb_dyno_topo_ix", "gb_dyno_metadata"],
    [enterdb_lib:open_table(Tab, false) || Tab <- Tabs],
    ?info("done opening intenal tables.").

-spec ensure_directories() ->
    ok.
ensure_directories() ->
    List = [ enterdb_lib:get_path(D) || D <- [db_path, wal_path, backup_path, checkpoint_path, hh_path, restore_path]],
    [ ok = filelib:ensure_dir(P) || P <- List, P =/= undefined],
    ok.
