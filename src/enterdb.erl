%%%-------------------------------------------------------------------
%%% @author erdem aksu <erdem@sitting>
%%% @copyright (C) 2015, Mobile Arts AB
%%% @doc
%%% Enterdb the key/value storage.
%%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
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

%%%===================================================================
%%% API
%%===================================================================

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
		    DB_PATH	= proplists:get_value(db_path, PropList),
		    NumOfShards = proplists:get_value(num_of_local_shards, PropList),
		    %% Specific table options
		    Wrapped	= proplists:get_value(wrapped, Options, undefined),
		    MemWrapped	= proplists:get_value(mem_wrapped, Options, undefined),
		    ?debug("Get Shards for: ~p, #:~p, wrapped: ~p, memwrapped ~p",
			    [Name, NumOfShards, Wrapped, MemWrapped]),
		    {ok, Shards} = enterdb_lib:get_shards(Name,
							  NumOfShards,
							  Wrapped,
							  MemWrapped),
		    Comp = proplists:get_value(comparator, Options, descending),
		    NewEDBT = EnterdbTab#enterdb_table{comparator = Comp,
						       path = DB_PATH,
						       shards = Shards},
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
	    Options = Table#enterdb_table.options,
	    Backend = proplists:get_value(backend, Options),
            case Backend of
		leveldb ->
		    enterdb_lib:open_leveldb_db(Table);
		ets_leveldb ->
		    enterdb_mem:init_tab(Name, Options),
		    enterdb_lib:open_leveldb_db(Table);
		Else ->
		    ?debug("enterdb:open_table: {backend, ~p} not supported", [Else]),
		    {error, "backend_not_supported"}
	    end;
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
	    Options = Table#enterdb_table.options,
	    Backend = proplists:get_value(backend, Options),
            case Backend of
		leveldb ->
		    enterdb_lib:close_leveldb_db(Table);
		Else ->
		    ?debug("enterdb:close_table: {backend, ~p} not supported", [Else]),
		    {error, "backend_not_supported"}
	    end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads Key from table with name Name
%% @end
%%--------------------------------------------------------------------
-spec read(Name :: string(),
           Key :: key()) -> {ok, value()} |
                          {error, Reason :: term()}.
read(Name, Key) ->
    case mochiglobal:get(list_to_atom(Name)) of
        undefined ->
            {error, "no_table"};
	#gb_hash_func{type = Type, ring = Ring}  ->
	    read_type(Type, Ring, Name, Key)
    end.

-spec read_from_disk(Name :: string(),
		     Key :: key()) -> ok | {error, Reason :: term()}.
read_from_disk(Name, Key) ->
    case mochiglobal:get(list_to_atom(Name)) of
	undefind ->
	    {error, "no_table"};
	#gb_hash_func{type = Type, ring = Ring} ->
	    read_type_from_disk(Type, Ring, Name, Key)
    end.

read_type_from_disk({_,FileMargin, TimeMargin}, Ring, Name, Key) ->
   read_type({tda, FileMargin, TimeMargin}, Ring, Name, Key).

read_type({mem_tda, FileMargin, TimeMargin}, Ring, Name, Key) ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    case enterdb_mem_wrp:read(Name, Ts, Key) of
		{error, _} = _E ->
		    %% Read from disk backend
		    Bucket = tda(FileMargin, TimeMargin, Ts),
		    {_, Level} = lists:keyfind(Bucket, 1, Ring),
		    %% Take another turn to find which shard for the level-table to be used.
		    {ok, Shard} = gb_hash:find_node(Level, Key),
		    enterdb_ldb_worker:read(Shard, Key);
		Res ->
		    Res
	    end
    end;
read_type({tda, FileMargin, TimeMargin}, Ring, _Name, Key) ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    Bucket = tda(FileMargin, TimeMargin, Ts),
	    {_, Level} = lists:keyfind(Bucket, 1, Ring),
	    %% Take another turn to find which shard for the level-table to be used.
	    {ok, Shard} = gb_hash:find_node(Level, Key),
	    enterdb_ldb_worker:read(Shard, Key)
    end;

read_type(Type, Ring, _Name, Key) ->
    {ok, Shard} = gb_hash:find_node(Ring, Type, Key),
    enterdb_ldb_worker:read(Shard, Key).
%%--------------------------------------------------------------------
%% @doc
%% Writes Key/Columns to table with name Name
%% @end
%%--------------------------------------------------------------------
-spec write(Name :: string(),
            Key :: key(),
            Columns :: [column()]) -> ok | {error, Reason :: term()}.
write(Name, Key, Columns) ->
    case mochiglobal:get(list_to_atom(Name)) of
        undefined ->
            {error, "no_table"};
	#gb_hash_func{type = Type, ring = Ring}  ->
	    write_type(Type, Ring, Name, Key, Columns)
    end.

-spec write_to_disk(Name :: string(),
		    Key :: key(),
		    Columns :: [column()]) -> ok | {error, Reason :: term()}.
write_to_disk(Name, Key, Columns) ->
    case mochiglobal:get(list_to_atom(Name)) of
	undefind ->
	    {error, "no_table"};
	#gb_hash_func{type = Type, ring = Ring} ->
	    write_type_to_disk(Type, Ring, Name, Key, Columns)
    end.

write_type_to_disk({mem_tda, FileMargin, TimeMargin}, Ring, Name, Key, Columns) ->
    %% Skip mem cache and write directly to disk (tda type)
    write_type({tda, FileMargin, TimeMargin}, Ring, Name, Key, Columns).

write_type({mem_tda, FileMargin, TimeMargin}, Ring, Name, Key, Columns) ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    case enterdb_mem_wrp:write(Name, Ts, Key, Columns) of
		{error, _} = _E ->
		    %% Write to disk backend
		    Bucket = tda(FileMargin, TimeMargin, Ts),
		    {_, Level} = lists:keyfind(Bucket, 1, Ring),
		    %% Take another turn to find which shard for the level-table to be used.
		    {ok, Shard} = gb_hash:find_node(Level, Key),
		    enterdb_ldb_worker:write(Shard, Key, Columns);
		Res ->
		    Res
	    end
    end;
write_type({tda, FileMargin, TimeMargin}, Ring, _Name, Key, Columns) ->
    case find_timestamp_in_key(Key) of
	undefined ->
	    undefined;
	{ok, Ts} ->
	    Bucket = tda(FileMargin, TimeMargin, Ts),
	    {_, Level} = lists:keyfind(Bucket, 1, Ring),
	    %% Take another turn to find which shard for the level-table to be used.
	    {ok, Shard} = gb_hash:find_node(Level, Key),
	    enterdb_ldb_worker:write(Shard, Key, Columns)
    end;
write_type(Type, Ring, _Name, Key, Columns) ->
    {ok, Shard} = gb_hash:find_node(Ring, Type, Key),
    enterdb_ldb_worker:write(Shard, Key, Columns).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from table with name Name
%% @end
%%--------------------------------------------------------------------
-spec delete(Name :: string(),
             Key :: key()) -> ok |
                            {error, Reason :: term()}.
delete(Name, Key)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
	{ok, {level, Level}} ->
	    {ok, Shard} = gb_hash:find_node(Level, Key),
	    enterdb_ldb_worker:delete(Shard, Key); 
        {ok, Shard} ->
            enterdb_ldb_worker:delete(Shard, Key)
    end.

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
    case enterdb_db:transaction(fun() -> atomic_delete_table(Name)  end) of
        {atomic, ok} ->
            ok;
        {atomic, {error, Reason}} ->
	    {error, Reason};
	{aborted, Reason} ->
            {error, Reason}
    end.

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
			path = Path,
			key = KeyDefinition,
			columns = ColumnsDefinition,
			indexes = IndexesDefinition,
			comparator = Comp,
			options = Options,
			shards = Shards
			}] ->
	    Backend = proplists:get_value(backend, Options),
	    {ok, Size} = enterdb_lib:approximate_size(Backend, Shards),
	    [{name, Name},
	     {path, Path},
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

-spec atomic_delete_table(Name :: string()) -> ok | {error, Reason :: term()}.
atomic_delete_table(Name) ->
    case mnesia:read(enterdb_table, Name) of
	[Table] ->
	    Options = Table#enterdb_table.options,
	    Backend = proplists:get_value(backend, Options),
	    case Backend of
		leveldb ->
		    ok = enterdb_lib:delete_leveldb_db(Table);
		ets_leveldb ->
		    ok = enterdb_lib:delete_leveldb_db(Table);
		Else ->
		    ?debug("enterdb:delete_table: {backend, ~p} not supported", [Else]),
		    {error, "backend_not_supported"}
	    end;
	[] ->
	    {error, "no_table"}
    end.

-spec find_timestamp_in_key(Key :: [{atom(), term()}]) -> undefined | {ok, Ts :: timestamp()}.
find_timestamp_in_key([])->
    undefined;
find_timestamp_in_key([{ts, Ts}|_Rest]) ->
    {ok, Ts};
find_timestamp_in_key([_|Rest]) ->
    find_timestamp_in_key(Rest).

-spec tda(FileMargin :: pos_integer(), TimeMargin :: pos_integer(), Ts :: timestamp()) ->
    Bucket :: integer().
tda(FileMargin, TimeMargin, {_, Seconds, _}) ->
    N = Seconds div TimeMargin,
    N rem FileMargin.
