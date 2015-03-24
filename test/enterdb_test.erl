-module(enterdb_test).


-export([open_db/1,
	 delete_db/1,
	 create_wrapping_db/1,
	 read/2,
	 read_range/4,
	 write/1,
	 write_loop/3,
	 write_server/3]).

-include("enterdb.hrl").

%%%===================================================================
%%% Library functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Open an existing enterdb db.
%% @end
%%--------------------------------------------------------------------
-spec open_db(Name :: string()) -> ok.
open_db(Name) ->
    enterdb_lib:open_leveldb_db(Name).

%%--------------------------------------------------------------------
%% @doc
%% Deletes the entry of database from enterdb_table. Does not delete
%% the actual table. TODO: Change when a proper enterdb:delete_db
%% function added.
%% @end
%%--------------------------------------------------------------------
-spec delete_db(Name :: string()) -> ok.
delete_db(Name) ->
    mnesia:dirty_delete(enterdb_table, Name).

%%--------------------------------------------------------------------
%% @doc
%% Creates a database which has a compound key with timestamp and imsi,
%% and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_wrapping_db(Name :: string()) -> ok.
create_wrapping_db(Name) ->
    Keys = [ts, imsi],
    Columns = [value],
    Indexes = [],
    Options = [{backend, leveldb},
	       {data_model,binary},
	       {time_ordered, true},
	       {wrapped, {16, 60}}], 
    enterdb:create_table(Name, Keys, Columns, Indexes, Options).

%%--------------------------------------------------------------------
%% @doc
%% Writes to table of given name generated key and data
%% based on Ts = erlang:now() where TS is an element of compound key.
%% @end
%%--------------------------------------------------------------------
-spec write(Name :: string()) -> ok.
write(Name) ->
    Ts = erlang:now(),
    EventKey = [{ts, Ts},
		{imsi, "240020000000001"}],
    {{YYYY, MM, DD}, {HH, Mm, SS}} = calendar:now_to_local_time(Ts),
    Value = lists:concat([YYYY,"-",MM,"-",DD," ",HH,":",Mm,":",SS]),
    EventValue = [{value, Value}],
    enterdb:write(Name, EventKey, EventValue).


%%--------------------------------------------------------------------
%% @doc
%% Read Key from enterdb db Name.
%% @end
%%--------------------------------------------------------------------
-spec read(Name :: string(), Key :: key()) -> {ok, Value :: value()} |
					      {error, Reason :: term()}.
read(Name, Key) ->
    enterdb:read(Name, Key).

%%--------------------------------------------------------------------
%% @doc
%% Read a range from StartKey to EndKey from enterdb db Name. Reads
%% from each shard is limited to given Limit
%% @end
%%--------------------------------------------------------------------
-spec read_range(Name :: string(),
		 StartKey :: key(),
		 EndKey :: key(),
		 Limit :: pos_integer()) ->
    {ok, [KVP :: kvp()]} |
    {error, Reason :: term()}.
read_range(Name, StartKey, EndKey, Limit) ->
    enterdb:read_range(Name, {StartKey, EndKey}, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Write N number of auto generated data with given interval in seconds
%% to the enterdb db that is provided by Name.
%% @end
%%--------------------------------------------------------------------
-spec write_loop(Name :: string(),
		 N :: pos_integer(),
		 Interval :: pos_integer()) ->
    {ok, Pid :: pid()} |
    {error, Reason :: term()}.
write_loop(Name, N, Interval) when N > 0 ->
    {ok, erlang:spawn(?MODULE, write_server, [Name, N, Interval*1000])}.

write_server(Name, N, Milliseconds) when N > 0 ->
    receive
	stop ->
	    ok
    after
	Milliseconds ->
	    write(Name),
	    write_server(Name, N-1, Milliseconds)
    end;
write_server(_,_,_) ->
    ok.
