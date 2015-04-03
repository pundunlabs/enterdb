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
         open_db/1,
	 close_db/1,
	 read/2,
         write/3,
         delete/2,
	 read_range/3,
	 delete_db/1]).

-export([load_test/0,
	 write_loop/1]).

-include("enterdb.hrl").
-include("gb_log.hrl").

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
%% Creates a table that is defined by Name, Key, Columns and optionally
%% Indexes.
%% Key is a list and if list has more than one element, then the key 
%% will ba a compound key.
%% Columns list consist of name of each column as atom and inclusion of
%% key columns are optional.
%% Indexes list are optional and an index table will be created for each
%% coulmn provided in this argument. Any given index column is not
%% neccesarly included in Columns.
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
	{ok, EnterdbTable} ->
	    case enterdb_server:get_state_params() of
		{ok, PropList}  ->
		    DB_PATH = proplists:get_value(db_path, PropList),
		    NumOfShards = proplists:get_value(num_of_local_shards, PropList),
		    Wrapped  = proplists:get_value(wrapped, Options, undefined),
		    ?debug("Get Shards for: ~p, #:~p, wrapped: ~p",
			    [Name, NumOfShards, Wrapped]),
		    {ok, Shards} = enterdb_lib:get_shards(Name,
							  NumOfShards,
							  Wrapped),
		    NewEDBT = EnterdbTable#enterdb_table{path = DB_PATH,
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
%% Open an existing enterdb database.
%% @end
%%--------------------------------------------------------------------
-spec open_db(Name :: string())-> ok | {error, Reason :: term()}.
open_db(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_table, Name) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [Table]} ->
	    Options = Table#enterdb_table.options,
	    Backend = proplists:get_value(backend, Options),
            case Backend of
		leveldb ->
		    enterdb_lib:open_leveldb_db(Table);
		Else ->
		    ?debug("enterdb:open_db: {backend, ~p} not supported", [Else]),
		    {error, "backend_not_supported"}
	    end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing enterdb database.
%% @end
%%--------------------------------------------------------------------
-spec close_db(Name :: string())-> ok | {error, Reason :: term()}.
close_db(Name) ->
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
		    ?debug("enterdb:close_db: {backend, ~p} not supported", [Else]),
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
read(Name, Key)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
	{ok, {level, Level}} ->
	    {ok, Shard} = gb_hash:find_node(Level, Key),
	    enterdb_ldb_worker:read(Shard, Key); 
        {ok, Shard} ->
            enterdb_ldb_worker:read(Shard, Key)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Writes Key/Columns to table with name Name
%% @end
%%--------------------------------------------------------------------
-spec write(Name :: string(),
            Key :: key(),
            Columns :: [column()]) -> ok | {error, Reason :: term()}.
write(Name, Key, Columns)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
	{ok, {level, Level}} ->
	    {ok, Shard} = gb_hash:find_node(Level, Key),
	    enterdb_ldb_worker:write(Shard, Key, Columns); 
        {ok, Shard} ->
            enterdb_ldb_worker:write(Shard, Key, Columns)
    end.

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
%% Reads a Range of Keys from table with name Name and returns mac Limit items
%% @end
%%--------------------------------------------------------------------
-spec read_range(Name :: string(),
		 Range :: key_range(),
		 Limit :: pos_integer()) -> {ok, [kvp()]} |
					    {error, Reason :: term()}.
read_range(Name, Range, Limit) ->
    enterdb_lib:read_range(Name, Range, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Delete a database completely. Ensures the database is closed before deletion.
%% @end
%%--------------------------------------------------------------------
-spec delete_db(Name :: string()) -> ok | {error, Reason :: term()}.
delete_db(Name) ->
    case enterdb_db:transaction(fun() -> atomic_delete_db(Name)  end) of
        {atomic, ok} ->
            ok;
        {atomic, {error, Reason}} ->
	    {error, Reason};
	{aborted, Reason} ->
            {error, Reason}
    end.

-spec atomic_delete_db(Name :: string()) -> ok | {error, Reason :: term()}.
atomic_delete_db(Name) ->
    case mnesia:read(enterdb_table, Name) of
	[Table] ->
	    Options = Table#enterdb_table.options,
	    Backend = proplists:get_value(backend, Options),
	    case Backend of
		leveldb ->
		    ok = enterdb_lib:delete_leveldb_db(Table);
		Else ->
		    ?debug("enterdb:close_db: {backend, ~p} not supported", [Else]),
		    {error, "backend_not_supported"}
	    end;
	[] ->
	    {error, "no_table"}
    end.
