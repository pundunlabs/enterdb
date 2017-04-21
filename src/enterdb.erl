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
         update/3,
	 delete/2,
	 read_range/3,
	 read_range_n/3,
	 delete_table/1,
	 write_to_disk/4,
	 table_info/1,
	 table_info/2,
	 first/1,
	 last/1,
	 seek/2,
	 next/1,
	 prev/1
	 ]).

-export([do_write/4,
	 do_update/4,
	 do_read/3,
	 do_write_to_disk/4,
	 do_read_from_disk/3,
	 do_delete/3]).

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
	    Type = maps:get(type, EnterdbTab, leveldb),
	    HashExclude = maps:get(hash_exclude, EnterdbTab, []),
	    Ts = maps:get(time_series, EnterdbTab, false),
	    HashKey = enterdb_lib:get_hash_key_def(KeyDef, HashExclude, Ts),
	    Dist = maps:get(distributed, EnterdbTab, true),
	    DataModel = maps:get(data_model, EnterdbTab, array),
	    RF = maps:get(replication_factor, EnterdbTab, 1),
	    HashingMethod = maps:get(hashing_method, EnterdbTab, uniform),
	    DefaultNOS = enterdb_lib:get_num_of_local_shards(),
	    NumberOfShards = maps:get(num_of_shards, EnterdbTab, DefaultNOS),
	    Comparator = maps:get(comparator, EnterdbTab, descending),
	    NewTab = EnterdbTab#{type => Type,
				 comparator => Comparator,
				 distributed => Dist,
				 data_model => DataModel,
				 hash_key => HashKey,
				 hashing_method => HashingMethod,
				 num_of_shards => NumberOfShards,
				 replication_factor => RF},
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
	TD = #{distributed := Dist} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    read_(Tab, Key, DB_HashKey, Dist);
	{error, _} = R ->
	    R
    end.

read_(Tab, Key, {ok, DBKey, HashKey}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_read, [Shard, Key, DBKey]}, read);
read_(Tab, Key, {ok, DBKey, HashKey}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_read(Shard, Key, DBKey);
read_(_Tab, _, {error, _} = E, _) ->
    E.

-spec read_from_disk(Name :: string(),
		     Key :: key()) ->
    ok | {error, Reason :: term()}.
read_from_disk(Tab, Key) ->
    case enterdb_lib:get_tab_def(Tab) of
	TD = #{} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    read_from_disk_(Tab, Key, DB_HashKey);
	{error, _} = R ->
	    R
    end.

%% Key ok according to keydef
read_from_disk_(Tab, Key, {ok, DBKey, HashKey}) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_read_from_disk(Shard, Key, DBKey);
%% Key not ok
read_from_disk_(_Tab, _Key, {error, _} = E) ->
    E.

do_read(Shard, Key, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    enterdb_lib:make_app_value(TD, do_read(TD, Shard, Key, DBKey)).

do_read_from_disk(Shard, Key, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    enterdb_lib:make_app_value(TD, do_read_from_disk(TD, Shard, Key, DBKey)).

%% internal read based on table / shard type
do_read(_TD = #{type := leveldb}, ShardTab, _Key, DBKey) ->
    enterdb_ldb_worker:read(ShardTab, DBKey);
do_read(_TD = #{type := leveldb_wrapped}, ShardTab, _Key, DBKey) ->
    enterdb_ldb_wrp:read(ShardTab, DBKey);
do_read(_TD = #{type := leveldb_tda,
		tda := Tda}, ShardTab, Key, DBKey) ->
    enterdb_ldb_tda:read(ShardTab, Tda, Key, DBKey);
do_read(_TD = #{type := rocksdb}, ShardTab, _Key, DBKey) ->
    enterdb_rdb_worker:read(ShardTab, DBKey);
do_read(_TD = #{type := Type}, _ShardTab, _Key, _DBKey) ->
    {error, {"read_not_supported", Type}};
do_read({error, R}, _, _, _) ->
    {error, R}.

do_read_from_disk(TD = #{type := Type}, ShardTab, _Key, DBKey)
    when Type =:= mem_leveldb ->
    enterdb_ldb_worker:read(TD, ShardTab, DBKey);

do_read_from_disk(TD = #{type := Type}, ShardTab, _Key, DBKey)
    when Type =:= mem_leveldb_wrapped ->
    enterdb_ldb_wrp:read(TD, ShardTab, DBKey);

do_read_from_disk(#{type := mem_leveldb_tda,
		    tda := Tda},
		  ShardTab, Key, DBKey) ->
    enterdb_ldb_tda:read(ShardTab, Tda, Key, DBKey);

%% Use ordinary read for table type
do_read_from_disk(TD, ShardTab, Key, DBKey) ->
    do_read(TD, ShardTab, Key, DBKey).

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
	TD = #{distributed := Dist} ->
	    DB_HashKeyAndCols = enterdb_lib:make_key_columns(TD, Key, Columns),
	    write_(Tab, Key, DB_HashKeyAndCols, Dist);
	{error, _} = R ->
	    R
    end.

-spec write_(Tab :: string(),
	     Key :: key(),
	     DB_Key_Columns :: {ok, DBKey :: binary(),
				HashKey :: binary(),
				DBColumns :: binary()} |
			       {error, Error :: term()},
	     Dist :: true | false) ->
    ok | {error, Reason :: term()}.
write_(Tab, Key, {ok, DBKey, HashKey, DBColumns}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_write, [Shard,Key,DBKey,DBColumns]}, write);
write_(Tab, Key, {ok, DBKey, HashKey, DBColumns}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_write(Shard, Key, DBKey, DBColumns);
write_(_Tab, _, {error, _} = E, _) ->
    E.

-spec write_to_disk(Name :: string(),
		    Key :: key(),
		    DBKey :: binary(),
		    DBColumns :: binary()) -> ok | {error, Reason :: term()}.
write_to_disk(Tab, Key, DBKey, DBColumns) ->
    case gb_hash:get_local_node(Tab, Key) of
	{ok, Shard} ->
	    do_write_to_disk(Shard, Key, DBKey, DBColumns);
	_ ->
	    {error, "no_table"}
    end.

do_write_to_disk(Shard, Key, DBKey, DBColumns) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_write_to_disk(TD, Shard, Key, DBKey, DBColumns).

do_write(Shard, Key, DBKey, DBColumns) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_write(TD, Shard, Key, DBKey, DBColumns).

do_write(_TD = #{type := Type}, ShardTab, Key, DBKey, DBColumns)
when Type =:= mem_leveldb_wrapped ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    case enterdb_mem_wrp:write(ShardTab, Ts, DBKey, DBColumns) of
		{error, _} = _E ->
		    %% Write to disk
		    enterdb_ldb_wrp:write(Ts, ShardTab, DBKey, DBColumns);
		Res ->
		    Res
	    end
    end;
do_write(#{type := leveldb_wrapped, wrapper := Wrapper},
	 ShardTab, _Key, DBKey, DBColumns) ->
    enterdb_ldb_wrp:write(ShardTab, Wrapper, DBKey, DBColumns);
do_write(#{type := leveldb_tda, tda := Tda},
	 ShardTab, Key, DBKey, DBColumns) ->
    enterdb_ldb_tda:write(ShardTab, Tda, Key, DBKey, DBColumns);
do_write(#{type := leveldb}, ShardTab, _Key, DBKey, DBColumns) ->
    enterdb_ldb_worker:write(ShardTab, DBKey, DBColumns);
do_write(#{type := mem_leveldb}, _Tab, _Key, _DBKey, _DBColumns) ->
    ok;
do_write(#{type := rocksdb}, ShardTab, _Key, DBKey, DBColumns) ->
    enterdb_rdb_worker:write(ShardTab, DBKey, DBColumns);
do_write({error, R}, _, _Key, _DBKey, _DBColumns) ->
    {error, R};
do_write(TD, Tab, Key, _DBKey, _DBColumns) ->
    ?debug("could not write ~p", [{TD, Tab, Key}]),
    {error, {bad_tab, {Tab, TD}}}.

do_write_to_disk(#{type := Type, wrapper := Wrapper},
		 ShardTab, _Key, DBKey, DBColumns)
    when Type =:= mem_leveldb_wrapped ->
    enterdb_ldb_wrp:write(ShardTab, Wrapper, DBKey, DBColumns);

do_write_to_disk(TD, ShardTab, Key, DBKey, DBColumns) ->
    do_write(TD, ShardTab, Key, DBKey, DBColumns).

%%--------------------------------------------------------------------
%% @doc
%% Updates Key according to operation definition Op.
%% field_name() :: string().
%% threshold() :: pos_integer().
%% setvalue() :: pos_integer().
%% update_instruction() :: increment |
%%			   {increment, threshold(), setvalue()} |
%%			   overwrite.
%% data() :: pos_integer() | term().
%% default() :: pos_integer() | term().
%% Op :: [{field_name(), instruction(), data()} |
%%	  {field_name(), instruction(), data(), default()}].
%% @end
%%--------------------------------------------------------------------
-spec update(Name :: string(),
	     Key :: key(),
	     Op :: update_op()) ->
    ok | {error, Reason :: term()}.
update(Tab, Key, Op) ->
    case enterdb_lib:get_tab_def(Tab) of
	#{data_model := kv} ->
	    {error, can_not_update_kv};
	TD = #{distributed := Dist} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    update_(Tab, Key, DB_HashKey, Op, Dist);
	{error, _} = R ->
	    R
    end.

-spec update_(Tab :: string(),
	      Key :: key(),
	      DB_HashKey :: {ok, DBKey :: binary(),
				 HashKey :: binary()} |
			    {error, Error :: term()},
	      Op :: update_op(),
	      Dist :: true | false) ->
    ok | {error, Reason :: term()}.
update_(Tab, Key, {ok, DBKey, HashKey}, Op, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_update, [Shard, Key, DBKey, Op]}, write);
update_(Tab, Key, {ok, DBKey, HashKey}, Op, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_update(Shard, Key, DBKey, Op);
update_(_Tab, _, {error, _} = E, _, _) ->
    E.

do_update(Shard, Key, DBKey, Op) ->
    TD = enterdb_lib:get_shard_def(Shard),
    enterdb_lib:make_app_value(TD, do_update(TD, Shard, Key, DBKey, Op)).

do_update(#{type := Type,
	    data_model := DataModel,
	    column_mapper := Mapper,
	    distributed := Dist}, Shard, Key, DBKey, Op)
when Type =:= mem_leveldb_wrapped ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    case enterdb_mem_wrp:update(Shard, Ts, DBKey, Op) of
		{error, _} = _E ->
		    %% Write to disk
		    enterdb_ldb_wrp:update(Shard, DBKey, Op,
					   DataModel, Mapper, Dist);
		Res ->
		    Res
	    end
    end;
do_update(#{type := leveldb_wrapped,
	    data_model := DataModel,
	    column_mapper := Mapper,
	    distributed := Dist}, Shard, _Key, DBKey, Op) ->
    enterdb_ldb_wrp:update(Shard, DBKey, Op, DataModel, Mapper, Dist);
do_update(TD = #{type := leveldb_tda}, _, Key, DBKey, Op) ->
    enterdb_ldb_tda:update(TD, Key, DBKey, Op);
do_update(#{type := leveldb,
	    data_model := DataModel,
	    column_mapper := Mapper,
	    distributed := Dist}, Shard, _Key, DBKey, Op) ->
    enterdb_ldb_worker:update(Shard, DBKey, Op, DataModel, Mapper, Dist);
do_update(_TD = #{type := mem_leveldb}, _Tab, _Key, _DBKey, _Op) ->
    ok;
do_update(#{type := rocksdb,
	    data_model := DataModel,
	    column_mapper := Mapper,
	    distributed := Dist}, Shard, _Key, DBKey, Op) ->
    enterdb_rdb_worker:update(Shard, DBKey, Op, DataModel, Mapper, Dist);
do_update({error, R}, _, _Key, _DBKey, _Op) ->
    {error, R};
do_update(TD, Tab, Key, _DBKey, _Op) ->
    ?debug("could not update ~p", [{TD, Tab, Key}]),
    {error, {bad_tab, {Tab,TD}}}.

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
	TD = #{distributed := Dist} ->
	    DB_HashKey = enterdb_lib:make_key(TD, Key),
	    delete_(Tab, Key, DB_HashKey, Dist);
	{error, _} = R ->
	    R
    end.

delete_(Tab, Key, {ok, DBKey, HashKey}, true) ->
    {ok, {Shard, Ring}} = gb_hash:get_node(Tab, HashKey),
    ?dyno:call(Ring, {?MODULE, do_delete, [Shard, Key, DBKey]}, write);
delete_(Tab, Key, {ok, DBKey, HashKey}, false) ->
    {ok, Shard} = gb_hash:get_local_node(Tab, HashKey),
    do_delete(Shard, Key, DBKey);
delete_(_Tab, _Key, {error, _} = E, _) ->
    E.

do_delete(Shard, Key, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_delete(TD, Shard, Key, DBKey).

%% internal read based on table / shard type
do_delete(_TD = #{type := leveldb}, ShardTab, _Key, DBKey) ->
    enterdb_ldb_worker:delete(ShardTab, DBKey);
do_delete(_TD = #{type := leveldb_wrapped}, ShardTab, _Key, DBKey) ->
    enterdb_ldb_wrp:delete(ShardTab, DBKey);
do_delete(_TD = #{type := leveldb_tda, tda := Tda}, ShardTab, Key, DBKey) ->
    enterdb_ldb_tda:delete(ShardTab, Tda, Key, DBKey);
do_delete(_TD = #{type := rocksdb}, ShardTab, _Key, DBKey) ->
    enterdb_rdb_worker:delete(ShardTab, DBKey);
do_delete(_TD = #{type := Type}, _ShardTab, _Key, _DBKey) ->
    {error, {delete_not_supported, Type}};
do_delete({error, R}, _, _, _) ->
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
	Tab = #{} ->
	    DBStartKey = enterdb_lib:make_key(Tab, StartKey),
	    DBStopKey = enterdb_lib:make_key(Tab, StopKey),
	    read_range_(Tab, DBStartKey, DBStopKey, Chunk);
	{error, _} = R ->
	    R
    end;
read_range(_, Range, _) ->
    {error, {badarg, Range}}.

-spec read_range_(Tab :: #{},
		  DBStartKey :: {ok, binary(), binary()},
		  DBStopKey :: {ok, binary(), binary()},
		  Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range_(Tab, {ok, DBStartK, _}, {ok, DBStopK, _}, Chunk) ->
    Shards = gb_hash:get_nodes(maps:get(name,Tab)),
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
	Tab = #{}->
	    DBKey = enterdb_lib:make_key(Tab, StartKey),
	    read_range_n_(Tab, DBKey, N);
	{error, _} = R ->
	    R
    end.


-spec read_range_n_(Tab :: #{},
		    {ok, DBKey :: binary(), binary()},
		    N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_(Tab, {ok, DBKey, _}, N) ->
    Shards = gb_hash:get_nodes(maps:get(name, Tab)),
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
			map = Map}] ->
	    Type = maps:get(type, Map),
	    Dist = maps:get(distributed, Map),
	    Shards = maps:get(shards, Map),
	    SizePL = get_size_param([size], Type, Shards, Dist),
	    ColumnsMapper = maps:get(column_mapper, Map),
	    ColumnsPL = get_columns_param([columns], ColumnsMapper),
	    Info = SizePL ++ ColumnsPL ++ maps:to_list(Map),
	    {ok, lists:keysort(1, Info)};
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
			map = Map}] ->
	    Type = maps:get(type, Map),
	    Dist = maps:get(distributed, Map),
	    Shards = maps:get(shards, Map),
	    SizePL = get_size_param(Parameters, Type, Shards, Dist),
	    ColumnsMapper = maps:get(column_mapper, Map),
	    ColumnsPL = get_columns_param(Parameters, ColumnsMapper),
	    List = SizePL ++ ColumnsPL ++ maps:to_list(Map),
	    Info = [ proplists:lookup(P, List) || P <- Parameters],
	    {ok, lists:keysort(1, Info)};
	[] ->
	    {error, "no_table"}
    end.

get_size_param(Parameters, Type, Shards, Dist) ->
    case lists:member(size, Parameters) of
	true ->
	    case enterdb_lib:approximate_size(Type, Shards, Dist) of
		{error, _Reason} ->
		    [];
		{ok, S} ->
		    [{size, S}]
	    end;
	false ->
	    []
    end.

get_columns_param(Parameters, ColumnsMapper) ->
    case lists:member(columns, Parameters) of
	true ->
	    case ColumnsMapper:entries() of
		Map ->
		    List = maps:to_list(Map),
		    Fun = fun(E) -> is_list(element(1,E)) end,
		    Filtered = lists:filter(Fun, List),
		    Sorted = lists:keysort(2, Filtered),
		    [{columns, [C || {C, _} <- Sorted]}]
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
    {ok, KVP :: kvp(), Ref :: binary()} |
    {error, Reason :: invalid | term()}.
first(Name) ->
    enterdb_it_worker:first(Name).

%%--------------------------------------------------------------------
%% @doc
%% Get the last Key/Value from table that is specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec last(Name :: string()) ->
    {ok, KVP :: kvp(), Ref :: binary()} |
    {error, Reason :: invalid | term()}.
last(Name) ->
    enterdb_it_worker:last(Name).

%%--------------------------------------------------------------------
%% @doc
%% Get the sought Key/Value from table that is specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec seek(Name :: string(), Key :: key()) ->
    {ok, KVP :: kvp(), Ref :: binary()} |
    {error, Reason :: invalid | term()}.
seek(Name, Key) ->
    enterdb_it_worker:seek(Name, Key).

%%--------------------------------------------------------------------
%% @doc
%% Get the next Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec next(Ref :: binary()) ->
    {ok, KVP :: kvp()} | {error, Reason :: invalid | term()}.
next(Ref) ->
    enterdb_it_worker:next(Ref).

%%--------------------------------------------------------------------
%% @doc
%% Get the prevoius Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec prev(Ref :: binary()) ->
    {ok, KVP :: kvp()} | {error, Reason :: invalid | term()}.
prev(Ref) ->
    enterdb_it_worker:prev(Ref).

-spec find_timestamp_in_key(Key :: [{string(), term()}]) ->
    undefined | {ok, Ts :: timestamp()}.
find_timestamp_in_key([])->
    undefined;
find_timestamp_in_key([{"ts", Ts}|_Rest]) ->
    {ok, Ts};
find_timestamp_in_key([_|Rest]) ->
    find_timestamp_in_key(Rest).
