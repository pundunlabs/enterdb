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
%% @doc
%% Enterdb the key/value storage.
%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%===================================================================

-module(enterdb).

%% API
-export([create_table/3,
         open_table/1,
	 close_table/1,
	 read/2,
         read_from_disk/2,
	 write/3,
         delete/2,
	 read_range/3,
	 read_range_n/3,
	 delete_table/1,
	 write_to_disk/3,
	 table_info/1,
	 table_info/2,
	 first/1,
	 last/1,
	 seek/2,
	 next/1,
	 prev/1
	 ]).

-export([do_write/3,
	 do_read/2,
	 do_write_to_disk/3,
	 do_read_from_disk/2,
	 do_delete/2]).

-export([load_test/0,
	 write_loop/1]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

load_test() ->
    %%enterdb_lib:open_leveldb_db("test_range").
    [spawn(?MODULE, write_loop, [10000000]) || _ <- lists:seq(1,8)].

write_loop(0) ->
    ok;
write_loop(N) when N > 0 ->
    Bin = [162,129,179,128,1,59,129,1,3,130,8,0,0,1,75,222,153,109,169,131,8,0,0,1,75,222,53,246,124,132,8,1,0,83,0,161,14,50,239,133,129,140,191,129,10,129,135,160,51,128,8,66,0,146,8,19,16,54,245,129,6,100,103,64,55,104,248,131,8,83,150,151,80,85,68,40,144,132,1,0,133,11,100,97,116,97,46,116,114,101,46,115,101,134,2,66,240,135,1,32,162,80,160,6,128,4,80,251,194,177,161,6,128,4,80,251,193,37,130,1,0,164,59,128,8,0,0,1,75,222,153,39,80,129,8,0,0,1,75,222,153,62,192,131,8,0,0,0,0,0,0,1,189,132,1,7,133,8,0,0,0,0,0,0,0,0,134,1,0,135,1,6,136,1,1,137,1,5,138,2,0,131],
    enterdb:write("test_range", [{ts, os:timestamp()}],[{value, Bin}]),
    write_loop(N-1).


%%--------------------------------------------------------------------
%% @doc
%% Creates a table that is defined by Name, KeyDef and given Options.
%% KeyDef is a list and if list has more than one element, then the key 
%% will be a compound key.
%% @end
%%--------------------------------------------------------------------
-spec create_table(Name :: string(), KeyDef :: [string()],
                   Options :: [table_option()])->
    ok | {error, Reason :: term()}.
create_table(Name, KeyDef, Options)->
    case enterdb_lib:verify_create_table_args([{name, Name},
					       {key, KeyDef},
					       {options, Options}]) of
	{ok, EnterdbTab} ->
	    %% Specific table options
	    Type = proplists:get_value(type, Options, leveldb),
	    DataModel = proplists:get_value(data_model, Options, map),
	    Mapper = enterdb_lib:get_column_mapper(Name, DataModel),
	    Comp = proplists:get_value(comparator, Options, descending),
	    Dist = proplists:get_value(distributed, Options, true),
	    HashKey = enterdb_lib:get_hash_key_def(KeyDef, Options),
	    NewTab = EnterdbTab#enterdb_table{column_mapper = Mapper,
					      comparator = Comp,
					      type = Type,
					      data_model = DataModel,
					      distributed = Dist,
					      hash_key = HashKey},
	    enterdb_lib:create_table(NewTab);
	{error, Reason} ->
                {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Open an existing enterdb database table.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Name :: string())-> ok | {error, Reason :: term()}.
open_table(Name) ->
    case gb_hash:is_distributed(Name) of
	undefined ->
            {error, "no_table"};
	Dist ->
	    enterdb_lib:open_table(Name, Dist)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing enterdb database table.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Name :: string())-> ok | {error, Reason :: term()}.
close_table(Name) ->
    case gb_hash:is_distributed(Name) of
	undefined ->
            {error, "no_table"};
	Dist ->
	    enterdb_lib:close_table(Name, Dist)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads Key from table with name Tab
%% @end
%%--------------------------------------------------------------------
-spec read(Tab :: string(),
           Key :: key()) -> {ok, value()} | {error, Reason :: term()}.
read(Tab, Key) ->
    case enterdb_lib:get_tab_def(Tab) of
	TD = #enterdb_table{distributed = Dist} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    read_(Tab, DB_HashKey, Dist);
	{error, _} = R ->
	    R
    end.

read_(Tab, {ok, DBKey, HashKey}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_read, [Shard, DBKey]}, read);
read_(Tab, {ok, DBKey, HashKey}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_read(Shard, DBKey);
read_(_Tab, {error, _} = E, _) ->
    E.

-spec read_from_disk(Name :: string(),
		     Key :: key()) ->
    ok | {error, Reason :: term()}.
read_from_disk(Tab, Key) ->
    case enterdb_lib:get_tab_def(Tab) of
	TD = #enterdb_table{} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    read_from_disk_(Tab, DB_HashKey);
	{error, _} = R ->
	    R
    end.

%% Key ok according to keydef
read_from_disk_(Tab, {ok, DBKey, HashKey}) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_read_from_disk(Shard, DBKey);
%% Key not ok
read_from_disk_(_Tab, {error, _} = E) ->
    E.

do_read(Shard, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    enterdb_lib:make_app_value(TD, do_read(TD, Shard, DBKey)).

do_read_from_disk(Shard, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    enterdb_lib:make_app_value(TD, do_read_from_disk(TD, Shard, DBKey)).

%% internal read based on table / shard type
do_read(_TD = #enterdb_stab{type = leveldb}, ShardTab, Key) ->
    enterdb_ldb_worker:read(ShardTab, Key);
do_read(_TD = #enterdb_stab{type = leveldb_wrapped}, ShardTab, Key) ->
    enterdb_ldb_wrp:read(ShardTab, Key);
do_read(_TD = #enterdb_stab{type = Type}, _ShardTab, _Key) ->
    {error, {"read_not_supported", Type}};
do_read({error, R}, _, _) ->
    {error, R}.

do_read_from_disk(TD = #enterdb_stab{type = Type}, ShardTab, Key) when
						    Type =:= ets_leveldb;
						    Type =:= ets_leveldb_wrapped ->
    enterdb_ldb_worker:read(TD, ShardTab, Key);

%% Use ordinary read for table type
do_read_from_disk(TD, ShardTab, Key) ->
    do_read(TD, ShardTab, Key).

%%--------------------------------------------------------------------
%% @doc
%% Writes Key/Columns to table with name Name
%% @end
%%--------------------------------------------------------------------
-spec write(Name :: string(),
            Key :: key(),
            Columns :: [column()]) -> ok | {error, Reason :: term()}.
write(Tab, Key, Columns) ->
    case enterdb_lib:get_tab_def(Tab) of
	    TD = #enterdb_table{distributed = Dist} ->
	    DB_HashKeyAndCols = enterdb_lib:make_key_columns(TD, Key, Columns),
	    write_(Tab, DB_HashKeyAndCols, Dist);
	{error, _} = R ->
	    R
    end.

-spec write_(Tab :: string(),
	     DB_Key_Columns :: {ok, DBKey :: binary(),
				HashKey :: binary(),
				DBColumns :: binary()} |
			       {error, Error :: term()},
	     Dist :: true | false) ->
    ok | {error, Reason :: term()}.
write_(Tab, {ok, DBKey, HashKey, DBColumns}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_write, [Shard, DBKey, DBColumns]}, write);
write_(Tab, {ok, DBKey, HashKey, DBColumns}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_write(Shard, DBKey, DBColumns);
write_(_Tab, {error, _} = E, _) ->
    E.

-spec write_to_disk(Name :: string(),
		    Key :: key(),
		    Columns :: [column()]) -> ok | {error, Reason :: term()}.
write_to_disk(Tab, Key, Columns) ->
    case gb_hash:get_local_node(Tab, Key) of
	{ok, Shard} ->
	    do_write_to_disk(Shard, Key, Columns);
	_ ->
	    {error, "no_table"}
    end.

do_write_to_disk(Shard, DBKey, DBColumns) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_write_to_disk(TD, Shard, DBKey, DBColumns).

do_write(Shard, DBKey, DBColumns) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_write(TD, Shard, DBKey, DBColumns).

do_write(_TD = #enterdb_stab{type = Type}, ShardTab, Key, Columns)
when Type =:= ets_leveldb_wrapped ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    case enterdb_mem_wrp:write(ShardTab, Ts, Key, Columns) of
		{error, _} = _E ->
		    %% Write to disk
		    enterdb_ldb_wrp:write(Ts, ShardTab, Key, Columns);
		Res ->
		    Res
	    end
    end;
do_write(_TD = #enterdb_stab{type = leveldb_wrapped,
			     wrapper = Wrapper},
	 ShardTab, Key, Columns) ->
    enterdb_ldb_wrp:write(ShardTab, Wrapper, Key, Columns);
do_write(_TD = #enterdb_stab{type = leveldb}, ShardTab, Key, Columns) ->
    enterdb_ldb_worker:write(ShardTab, Key, Columns);
do_write(_TD = #enterdb_stab{type = ets_leveldb}, _Tab, _Key, _Columns) ->
    ok;
do_write({error, R}, _, _Key, _Columns) ->
    {error, R};
do_write(TD, Tab, Key, _Columns) ->
    ?debug("could not write ~p", [{TD, Tab, Key}]),
    {error, {bad_tab, {Tab,TD}}}.

do_write_to_disk(#enterdb_stab{type = Type,
			       wrapper = Wrapper},
		 ShardTab, Key, Columns)
    when Type =:= ets_leveldb_wrapped ->
    enterdb_ldb_wrp:write(ShardTab, Wrapper, Key, Columns);

do_write_to_disk(TD, ShardTab, Key, Columns) ->
    do_write(TD, ShardTab, Key, Columns).


%%--------------------------------------------------------------------
%% @doc
%% Delete Key from table with name Name
%% @end
%%--------------------------------------------------------------------
-spec delete(Name :: string(),
             Key :: key()) -> ok |
                            {error, Reason :: term()}.
delete(Tab, Key) ->
    case enterdb_lib:get_tab_def(Tab) of
	TD = #enterdb_table{distributed = Dist} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    delete_(Tab, DB_HashKey, Dist);
	{error, _} = R ->
	    R
    end.

delete_(Tab, {ok, DBKey, HashKey}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_delete, [Shard, DBKey]}, write);
delete_(Tab, {ok, DBKey, HashKey}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_delete(Shard, DBKey);
delete_(_Tab, {error, _} = E, _) ->
    E.

do_delete(Shard, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_delete(TD, Shard, DBKey).

%% internal read based on table / shard type
do_delete(_TD = #enterdb_stab{type = leveldb}, ShardTab, Key) ->
    enterdb_ldb_worker:delete(ShardTab, Key);
do_delete(_TD = #enterdb_stab{type = leveldb_wrapped}, ShardTab, Key) ->
    enterdb_ldb_wrp:delete(ShardTab, Key);
do_delete(_TD = #enterdb_stab{type = Type}, _ShardTab, _Key) ->
    {error, {delete_not_supported, Type}};
do_delete({error, R}, _, _) ->
    {error, R}.

%%--------------------------------------------------------------------
%% @doc
%% Reads a Range of Keys from table with name Name and returns max
%% Chunk items from each local shard of the table
%% @end
%%--------------------------------------------------------------------
-spec read_range(Name :: string(),
		 Range :: key_range(),
		 Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range(Name, {StartKey, StopKey}, Chunk) ->
    case enterdb_lib:get_tab_def(Name) of
	Tab = #enterdb_table{} ->
	    DBStartKey = enterdb_lib:make_key(Tab, StartKey),
	    DBStopKey = enterdb_lib:make_key(Tab, StopKey),
	    read_range_(Tab, DBStartKey, DBStopKey, Chunk);
	{error, _} = R ->
	    R
    end;
read_range(_, Range, _) ->
    {error, {badarg, Range}}.

-spec read_range_(Name :: string(),
		   DBStartKey :: {ok, binary(), binary()},
		   DBStopKey :: {ok, binary(), binary()},
		   Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range_(Tab, {ok, DBStartK, _}, {ok, DBStopK, _}, Chunk) ->
    Shards = gb_hash:get_nodes(Tab#enterdb_table.name),
    enterdb_lib:read_range_on_shards(Shards, Tab, {DBStartK, DBStopK}, Chunk);
read_range_(_Tab, {error, _} = E, _, _N) ->
    E;
read_range_(_Tab, _, {error, _} = E, _N) ->
    E.

%%--------------------------------------------------------------------
%% @doc
%% Reads N nuber of Keys from table with name Name starting form
%% StartKey.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n(Name :: string(),
		   StartKey :: key(),
		   N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n(Name, StartKey, N) ->
    case enterdb_lib:get_tab_def(Name) of
	Tab = #enterdb_table{} ->
	    DBKey = enterdb_lib:make_key(Tab, StartKey),
	    read_range_n_(Tab, DBKey, N);
	{error, _} = R ->
	    R
    end.


-spec read_range_n_(Tab :: #enterdb_table{},
		    {ok, DBKey :: binary(), binary()},
		    N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_(Tab, {ok, DBKey, _}, N) ->
    Shards = gb_hash:get_nodes(Tab#enterdb_table.name),
    enterdb_lib:read_range_n_on_shards(Shards, Tab, DBKey, N);
read_range_n_(_Tab, {error, _} = E, _N) ->
    E.

%%--------------------------------------------------------------------
%% @doc
%% Delete a database table completely. Ensures the table is closed 
%% before deletion.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Name :: string()) -> ok | {error, Reason :: term()}.
delete_table(Name) ->
    case gb_hash:is_distributed(Name) of
	undefined ->
            {error, "no_table"};
	Dist ->
	    enterdb_lib:delete_table(Name, Dist)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get information on table's configuration and size of the stored
%% data in the table
%% @end
%%--------------------------------------------------------------------
-spec table_info(Name :: string()) ->
    {ok, [{atom(), term()}]} | {error, Reason :: term()}.
table_info(Name) ->
    case mnesia:dirty_read(enterdb_table, Name) of
	[#enterdb_table{name = Name,
			path = _Path,
			key = KeyDefinition,
			column_mapper = Mapper,
			comparator = Comp,
			options = Options,
			shards = Shards,
			distributed = Dist
			}] ->
	    SizePL = get_size_param([size], Options, Shards, Dist),
	    Rest = SizePL ++ Options,
	    {ok, [{name, Name},
		  {key, KeyDefinition},
		  {columns_mapper, Mapper},
		  {comparator, Comp} | Rest]};
	[] ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get information on table for given parameters.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Name :: string(), Parameters :: [atom()]) ->
    {ok, [{atom(), term()}]} | {error, Reason :: term()}.
table_info(Name, Parameters) ->
    case mnesia:dirty_read(enterdb_table, Name) of
	[#enterdb_table{name = Name,
			path = Path,
			key = KeyDefinition,
			column_mapper = Mapper,
			comparator = Comp,
			data_model = DataModel,
			options = Options,
			shards = Shards,
			distributed = Dist
			}] ->
	    IList = [{name, Name},
		    {path, Path},
		    {key, KeyDefinition},
		    {column_mapper, Mapper},
		    {comparator, Comp},
		    {data_model, DataModel},
		    {shards, Shards},
		    {distributed, Dist} | Options],
	    SizePL = get_size_param(Parameters, Options, Shards, Dist),
	    List = SizePL ++ IList,
	    {ok, [ proplists:lookup(P, List) || P <- Parameters]};
	[] ->
	    {error, "no_table"}
    end.

get_size_param(Parameters, Options, Shards, Dist) ->
    case lists:member(size, Parameters) of
	true ->
	    Type = proplists:get_value(type, Options),
	    case enterdb_lib:approximate_size(Type, Shards, Dist) of
		{error, _Reason} ->
		    [];
		{ok, S} ->
		    [{size, S}]
	    end;
	false ->
	    []
    end.


%%--------------------------------------------------------------------
%% @doc
%% Get the first Key/Value from table that is specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec first(Name :: string()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
first(Name) ->
    enterdb_lit_worker:first(Name).

%%--------------------------------------------------------------------
%% @doc
%% Get the last Key/Value from table that is specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec last(Name :: string()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
last(Name) ->
    enterdb_lit_worker:last(Name).

%%--------------------------------------------------------------------
%% @doc
%% Get the sought Key/Value from table that is specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec seek(Name :: string(), Key :: key()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
seek(Name, Key) ->
    enterdb_lit_worker:seek(Name, Key).

%%--------------------------------------------------------------------
%% @doc
%% Get the next Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec next(Ref :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: invalid | term()}.
next(Ref) ->
    enterdb_lit_worker:next(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Get the prevoius Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec prev(Ref :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: invalid | term()}.
prev(Ref) ->
    enterdb_lit_worker:prev(Ref).

-spec find_timestamp_in_key(Key :: [{string(), term()}]) ->
    undefined | {ok, Ts :: timestamp()}.
find_timestamp_in_key([])->
    undefined;
find_timestamp_in_key([{"ts", Ts}|_Rest]) ->
    {ok, Ts};
find_timestamp_in_key([_|Rest]) ->
    find_timestamp_in_key(Rest).
