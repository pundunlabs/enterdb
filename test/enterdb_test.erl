%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2015 Pundun Labs AB
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
%% Module Description: Some example test functions for enterdb.
%% @end
%%%===================================================================

-module(enterdb_test).

-export([open_table/1,
	 delete_table/1,
	 create_table/1,
	 create_wrapping_table/1,
	 create_mem_wrapping_table/1,
	 create_mem_wrapping_table_2/1,
	 create_write_read_delete/1,
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
%% Open an existing enterdb table.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Name :: string()) -> ok.
open_table(Name) ->
    enterdb:open_table(Name).

%%--------------------------------------------------------------------
%% @doc
%% Deletes the entire database table specified by name.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Name :: string()) -> ok.
delete_table(Name) ->
    enterdb:delete_table(Name).

%%--------------------------------------------------------------------
%% @doc
%% Creates a database table which has a compound key with timestamp
%% and imsi, and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_table(Name :: string()) -> ok.
create_table(Name) ->
    Keys = [ts, imsi],
    Columns = [value],
    Indexes = [],
    Options = [{type, leveldb},
	       {data_model,binary}],
    enterdb:create_table(Name, Keys, Columns, Indexes, Options).

%%--------------------------------------------------------------------
%% @doc
%% Creates a database table which has a compound key with timestamp
%% and imsi, and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_write_read_delete(Name :: string()) -> ok.
create_write_read_delete(Name) ->
    Keys = [ts, imsi],
    Columns = [value],
    Indexes = [],
    Options = [{type, leveldb},
	       {data_model,binary}],
    ok = enterdb:create_table(Name, Keys, Columns, Indexes, Options),
    %% write data 1
    TS = {ts, {1,2,3}},
    Data = ["data hej!"],
    IMSI = {imsi, "hej"},
    ok = enterdb:write(Name, [IMSI, TS], Data),
    {ok, Data} = enterdb:read(Name, [IMSI, TS]),
    {ok, Data} = enterdb:read(Name, [TS, IMSI]),

    %% write data 2
    Data2 = ["data hej2!"],
    IMSI2 = {imsi, "hej2"},
    ok = enterdb:write(Name, [IMSI2, TS], Data2),

    %% write data 3
    Data3 = ["data hej3!"],
    IMSI3 = {imsi, "hej3"},
    ok = enterdb:write(Name, [IMSI3, TS], Data3),

    %% read range
    {ok,[{[TS,IMSI3],Data3},
	 {[TS,IMSI2],Data2},
	 {[TS,IMSI] ,Data}]} =
	enterdb:read_range_n(Name, [IMSI3, TS], 3),

    %% delete table
    ok = enterdb:delete_table(Name),
    {error, "no_table"} = enterdb:table_info(Name),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Creates a database table which has a compound key with timestamp
%% and imsi, and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_wrapping_table(Name :: string()) -> ok.
create_wrapping_table(Name) ->
    Keys = [ts, imsi],
    Columns = [value],
    Indexes = [],
    Options = [{type, leveldb_wrapped},
	       {data_model,binary},
	       {wrapper, #enterdb_wrapper{num_of_buckets = 10, size_margin = {megabytes, 10}}}],
    enterdb:create_table(Name, Keys, Columns, Indexes, Options).

%%--------------------------------------------------------------------
%% @doc
%% Creates a mem_wrapping table which has a compound key with timestamp
%% and imsi, and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_mem_wrapping_table(Name :: string()) -> ok.
create_mem_wrapping_table(Name) ->
    Keys = [ts, imsi],
    Columns = [value],
    Indexes = [],
    Options = [{type, ets_leveldb},
	       {data_model,binary},
	       {mem_wrapped, {5, 12}},
	       {wrapped, {16, 60}}],
    enterdb:create_table(Name, Keys, Columns, Indexes, Options).

%%--------------------------------------------------------------------
%% @doc
%% Creates a mem_wrapping table which has a compound key with timestamp
%% and imsi, and wrapping on files based on timestamp in the key.
%% @end
%%--------------------------------------------------------------------
-spec create_mem_wrapping_table_2(Name :: string()) -> ok.
create_mem_wrapping_table_2(Name) ->
    Keys = [ts, imsi],
    Columns = [value1, value2],
    Indexes = [],
    Options = [{type, ets_leveldb},
	       {data_model,binary},
	       {mem_wrapped, {2, 3}},
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
    {ok, erlang:spawn(?MODULE, write_server, [Name, N, Interval])}.

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
    io:format("write_server done ~p~n", [self()]),
    ok.
