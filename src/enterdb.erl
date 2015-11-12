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
%% Enterdb the key/value storage.
%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%===================================================================

-module(enterdb).

%% API
-export([create_table/5,
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
-include("gb_log.hrl").

-include("gb_hash.hrl").

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
%% Creates a table that is defined by Name, KeyDef, ColumnsDef and
%% optionally IndexesDef.
%% KeyDef is a list and if list has more than one element, then the key 
%% will be a compound key.
%% ColumnsDef list consist of name of each column as atom. Inclusion of
%% key in ColumnsDef list is optional.
%% IndexesDef list are optional and an index table will be created for each
%% coulmn provided in this argument. Any given index column is not
%% neccesarly included in ColumnsDef list.
%% @end
%%--------------------------------------------------------------------
-spec create_table(Name :: string(), KeyDef :: [atom()],
                   ColumnsDef :: [atom()], IndexesDef :: [atom()],
                   Options :: [table_option()])->
    ok | {error, Reason :: term()}.
create_table(Name, KeyDef, ColumnsDef, IndexesDef, Options)->
    case enterdb_lib:verify_create_table_args([{name, Name},
					       {key, KeyDef},
					       {columns, ColumnsDef},
					       {indexes,IndexesDef},
					       {options, Options}]) of
	{ok, EnterdbTab} ->
	    case enterdb_server:get_state_params() of
		{ok, PropList}  ->
		    NumOfShards0 = proplists:get_value(num_of_local_shards, PropList),
		    %% Specific table options
		    Type	= proplists:get_value(type, Options, leveldb),
		    NumOfShards	= proplists:get_value(shards, Options, NumOfShards0),
		    Nodes	= proplists:get_value(nodes, Options, [node()]),
		    DataModel	= proplists:get_value(data_model, Options),
		    Comp = proplists:get_value(comparator, Options, descending),
		    {ok, Shards} = enterdb_lib:get_shards(Name,
							  NumOfShards,
							  Nodes),
		    ?debug("table allocated shards ~p", [Shards]),
		    NewEDBT = EnterdbTab#enterdb_table{comparator = Comp,
						       shards = Shards,
						       type = Type,
						       data_model = DataModel},

		    enterdb_lib:create_table(NewEDBT);
		Else ->
		    {error, Else}
	    end;
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
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_table, Name) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [Table]} ->
	    enterdb_lib:open_db(Table);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing enterdb database table.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Name :: string())-> ok | {error, Reason :: term()}.
close_table(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_table, Name) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [Table]} ->
	    enterdb_lib:close_db(Table);
        {error, Reason} ->
            {error, Reason}
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
	TD = #enterdb_table{} ->
	    DBKey = enterdb_lib:make_key(TD, Key),
	    read_(Tab, DBKey);
	{error, _} = R ->
	    R
    end.

read_(Tab, {ok, DBKey}) ->
    {ok, {Node, Shard}} = gb_hash:get_node(Tab, DBKey),
    rpc:call(Node, ?MODULE, do_read, [Shard, DBKey]);

read_(_Tab, {error, _} = E) ->
    E.

-spec read_from_disk(Name :: string(),
		     Key :: key()) -> ok | {error, Reason :: term()}.
read_from_disk(Tab, Key) ->
    case enterdb_lib:get_tab_def(Tab) of
	TD = #enterdb_table{} ->
	    DBKey = enterdb_lib:make_key(TD, Key),
	    read_from_disk_(Tab, DBKey);
	{error, _} = R ->
	    R
    end.
%% Key ok according to keydef
read_from_disk_(Tab, {ok, DBKey}) ->
    {ok, {Node, Shard}} = gb_hash:get_node(Tab, DBKey),
    rpc:call(Node, ?MODULE, do_read_from_disk, [Shard, DBKey]);
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
	    TD = #enterdb_table{} ->
	    DBKeyAndCols = enterdb_lib:make_key_columns(TD, Key, Columns),
	    write_(Tab, DBKeyAndCols);
	{error, _} = R ->
	    R
    end.

-spec write_(Tab :: string(),
	     DB_Key_Columns :: {ok, DBKey :: binary(), DBColumns :: binary()} |
			       {error, Error :: term()} ) ->
    ok | {error, Reason :: term()}.
write_(Tab, {ok, DBKey, DBColumns}) ->
    {ok, {Node, Shard}} = gb_hash:get_node(Tab, DBKey),
    rpc:call(Node, ?MODULE, do_write, [Shard, DBKey, DBColumns]);
write_(_Tab, {error, _} = E) ->
    E.

-spec write_to_disk(Name :: string(),
		    Key :: key(),
		    Columns :: [column()]) -> ok | {error, Reason :: term()}.
write_to_disk(Tab, Key, Columns) ->
    case gb_hash:get_node(Tab, Key) of
	{ok, {Node, Shard}} ->
	    rpc:call(Node, ?MODULE, do_write_to_disk, [Shard, Key, Columns]);
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
	TD = #enterdb_table{} ->
	    DBKey = enterdb_lib:make_key(TD, Key),
	    delete_(Tab, DBKey);
	{error, _} = R ->
	    R
    end.

delete_(Tab, {ok, DBKey}) ->
    {ok, {Node, Shard}} = gb_hash:get_node(Tab, DBKey),
    rpc:call(Node, ?MODULE, do_delete, [Shard, DBKey]);

delete_(_Tab, {error, _} = E) ->
    E.

do_delete(Shard, DBKey) ->
    TD = enterdb_lib:get_shard_def(Shard),
    do_delete(TD, Shard, DBKey).

%% internal read based on table / shard type
do_delete(_TD = #enterdb_stab{type = leveldb}, ShardTab, Key) ->
    enterdb_ldb_worker:delete(ShardTab, Key);
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
read_range(Name, Range, Chunk) ->
    enterdb_lib:read_range(Name, Range, Chunk).

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
    enterdb_lib:read_range_n(Name, StartKey, N).

%%--------------------------------------------------------------------
%% @doc
%% Delete a database table completely. Ensures the table is closed before deletion.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Name :: string()) -> ok | {error, Reason :: term()}.
delete_table(Name) ->
    do_table_delete(Name).

%%--------------------------------------------------------------------
%% @doc
%% Get information on table's configuration and size of the stored
%% data in the table
%% @end
%%--------------------------------------------------------------------
-spec table_info(Name :: string()) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Name) ->
    case mnesia:dirty_read(enterdb_table, Name) of
	[#enterdb_table{name = Name,
			path = _Path,
			key = KeyDefinition,
			columns = ColumnsDefinition,
			indexes = IndexesDefinition,
			comparator = Comp,
			options = Options,
			shards = Shards
			}] ->
	    Type = proplists:get_value(type, Options),
	    {ok, Size} = enterdb_lib:approximate_size(Type, Shards),
	    [{name, Name},
	     {key, KeyDefinition},
	     {columns, ColumnsDefinition},
	     {indexes, IndexesDefinition},
	     {comparator, Comp},
	     {size, Size} | Options];
	[] ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get information on table for given parameters.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Name :: string(), Parameters :: [atom()]) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Name, Parameters) ->
    case mnesia:dirty_read(enterdb_table, Name) of
	[#enterdb_table{name = Name,
			path = Path,
			key = KeyDefinition,
			columns = ColumnsDefinition,
			indexes = IndexesDefinition,
			comparator = Comp,
			options = Options,
			shards = Shards
			}] ->
	    List = [{name, Name},
		    {path, Path},
		    {key, KeyDefinition},
		    {columns, ColumnsDefinition},
		    {indexes, IndexesDefinition},
		    {comparator, Comp},
		    {shards, Shards} | Options],
	    [ proplists:lookup(P, List) || P <- Parameters];
	[] ->
	    {error, "no_table"}
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

-spec do_table_delete(Name :: string()) -> ok | {error, Reason :: term()}.
do_table_delete(Name) ->
    case mnesia:dirty_read(enterdb_table, Name) of
	[Table] ->
	    Shards = Table#enterdb_table.shards,
	    ok = enterdb_lib:delete_shards(Shards),
	    ok = enterdb_lib:cleanup_table(Name, Shards);
	_ ->
	    {error, no_such_table}
    end.

-spec find_timestamp_in_key(Key :: [{atom(), term()}]) -> undefined | {ok, Ts :: timestamp()}.
find_timestamp_in_key([])->
    undefined;
find_timestamp_in_key([{ts, Ts}|_Rest]) ->
    {ok, Ts};
find_timestamp_in_key([_|Rest]) ->
    find_timestamp_in_key(Rest).
