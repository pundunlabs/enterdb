%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%% LevelDB backend worker module
%%% @end
%%% Created : 16 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_ldb_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([read/2,
         write/3,
         delete/2,
	 delete_db/1,
	 read_range_binary/3,
	 read_range_term/3,
	 read_range_n_binary/3,
	 recreate_shard/1,
	 approximate_sizes/2,
	 approximate_size/1,
	 get_iterator/1]).

-define(SERVER, ?MODULE).

-include("enterdb.hrl").
-include("gb_log.hrl").

-record(state, {db_ref,
                options,
		readoptions,
                writeoptions,
                name,
                key,
                columns,
                indexes,
		is_empty,
                time_ordered,
		wrapped,
		data_model,
		path,
		subdir,
		shard_name,
		options_pl}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a server that manages an enterdb shard with leveldb backend.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args :: [{atom(), term()} | atom()]) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(Args) ->
    Name = list_to_atom(proplists:get_value(name, Args)),
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Read Key from given shard.
%% @end
%%--------------------------------------------------------------------
-spec read(Shard :: string(),
           Key :: key()) -> {ok, Value :: term()} |
                            {error, Reason :: term()}.
read(Shard, Key) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Coulumns to given shard.
%% @end
%%--------------------------------------------------------------------
-spec write(Shard :: string(),
            Key :: key(),
            Columns :: [column()]) -> ok | {error, Reason :: term()}.
write(Shard, Key, Columns) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {write, Key, Columns}).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
             Key :: key()) -> ok | {error, Reason :: term()}.
delete(Shard, Key) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {delete, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Delete the database specified by given Shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec delete_db(Shard :: string()) -> ok | {error, Reason::term()}.
delete_db(Shard) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, delete_db).

%%--------------------------------------------------------------------
%% @doc
%% Read range of keys from a given shard and return max Chunk of
%% items in binary format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_binary(Shard :: string(),
			Range :: key_range(),
			Chunk :: pos_integer()) ->
    {ok, [{binary(), binary()}], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range_binary(Shard, Range, Chunk) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read_range, Range, Chunk, binary}).

%%--------------------------------------------------------------------
%% @doc
%% Read range of keys from a given shard and return max Chunk of
%% items in erlang term format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_term(Shard :: string(),
		      Range :: key_range(),
		      Chunk :: pos_integer()) ->
    {ok, [{binary(), binary()}], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range_term(Shard, Range, Chunk) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read_range, Range, Chunk, term}).

%%--------------------------------------------------------------------
%% @doc
%% Read N number of keys from a given shard and return read
%% items in binary format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n_binary(Shard :: string(),
			  StartKey :: key(),
			  N :: pos_integer()) ->
    {ok, [{binary(), binary()}]} | {error, Reason :: term()}.
read_range_n_binary(Shard, StartKey, N) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read_range_n, StartKey, N, binary}).

%%--------------------------------------------------------------------
%% @doc
%% Recreate the shard by destroying the leveldb db and re-open a new
%% one
%% @end
%%--------------------------------------------------------------------
-spec recreate_shard(Shard :: string()) -> ok.
recreate_shard(Shard) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, recreate_shard).

%%--------------------------------------------------------------------
%% @doc
%% Approximate the size of leveldb db in bytes for given key ranges.
%% @end
%%--------------------------------------------------------------------
-spec approximate_sizes(Shard :: string(), Ranges :: key_range()) ->
    {ok, Sizes :: [pos_integer()]} | {error, Reason :: term()}.
approximate_sizes(Shard, Ranges) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {approximate_sizes, Ranges}).

%%--------------------------------------------------------------------
%% @doc
%% Approximate the size of leveldb db in bytes.
%% @end
%%--------------------------------------------------------------------
-spec approximate_size(Shard :: string()) ->
    {ok, Size :: pos_integer()} | {error, Reason :: term()}.
approximate_size(Shard) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, approximate_size).

%%--------------------------------------------------------------------
%% @doc
%% Get an iterator resource from leveldb backend.
%% @end
%%--------------------------------------------------------------------
-spec get_iterator(Shard :: string()) ->
    {ok, It :: it()} | {error, Reason :: term()}.
get_iterator(Shard) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, get_iterator).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
    Name = proplists:get_value(name, Args),
    Subdir = proplists:get_value(subdir, Args),
    Path = enterdb_server:get_db_path(),
    ok = ensure_closed(Name),
 
    OptionsPL = proplists:get_value(options, Args),
    
    TabRec = proplists:get_value(tab_rec, Args),
    #enterdb_stab{shard = ShardName,
                  key = Key,
		  columns = Columns,
		  indexes = Indexes,
		  data_model = DataModel} = TabRec,

    OptionsRec = build_leveldb_options(OptionsPL),

    case leveldb:options(OptionsRec) of
        {ok, Options} ->
            FullPath = filename:join([Path, Subdir, ShardName]),
            ok = filelib:ensure_dir(FullPath),
	    case leveldb:open_db(Options, FullPath) of
                {error, Reason} ->
                    {stop, {error, Reason}};
                {ok, DB} ->
		    ELR = #enterdb_ldb_resource{name = Name, resource = DB},
                    ok = write_enterdb_ldb_resource(ELR),
		    ReadOptionsRec = build_leveldb_readoptions([]),
                    {ok, ReadOptions} = leveldb:readoptions(ReadOptionsRec),
                    WriteOptionsRec = build_leveldb_writeoptions([]),
                    {ok, WriteOptions} = leveldb:writeoptions(WriteOptionsRec),
                    process_flag(trap_exit, true),
		    {ok, #state{db_ref = DB,
                                options = Options,
				readoptions = ReadOptions,
                                writeoptions = WriteOptions,
                                name = Name,
                                key = Key,
                                columns = Columns,
                                indexes = Indexes,
                                %%TODO: Add leveldb:is_empty(DB) to check. Use iterator. 
				is_empty = true,
				data_model = DataModel,
                                path = Path,
				subdir = Subdir,
				shard_name = ShardName,
				options_pl = OptionsPL}}
            end;
        {error, Reason} ->
            {stop, {error, Reason}}
    end. 
                                   
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({read, DBKey}, _From,
            State = #state{db_ref = DB,
                           readoptions = ReadOptions}) ->
    Reply = leveldb:get(DB, ReadOptions, DBKey),
    {reply, Reply, State};
handle_call({write, Key, Columns}, From,
	    State = #state{is_empty = true,
			   wrapped = {_, TimeMargin},
			    shard_name = ShardName}) ->
    ok = enterdb_server:wrap_level(?MODULE, ShardName, Key, TimeMargin),
    handle_call({write, Key, Columns}, From,
		State#state{is_empty = false});
handle_call({write, Key, Columns}, _From, State) ->
    #state{db_ref = DB,
           writeoptions = WriteOptions} = State,
    Reply = leveldb:put(DB, WriteOptions, Key, Columns),
    {reply, Reply, State};
handle_call({delete, DBKey}, _From, State) ->
    #state{db_ref = DB,
           writeoptions = WriteOptions,
           key = KeyDef} = State,
    Reply = leveldb:delete(DB, WriteOptions, DBKey),
    {reply, Reply, State};
handle_call({read_range, Keys, Chunk, Type}, _From, State) when Chunk > 0 ->
   #state{db_ref = DB,
	  options = Options,
	  readoptions = ReadOptions} = State,
    Reply = leveldb:read_range(DB, Options, ReadOptions, Keys, Chunk),
    {reply, Reply, State};
handle_call({read_range_n, StartKey, N, _Type}, _From, State) when N >= 0 ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    Reply = leveldb:read_range_n(DB, ReadOptions, StartKey, N),
    {reply, Reply, State};
handle_call(get_data_model, _From, State = #state{key = KeyDef,
						  columns = ColumnsDef,
						  data_model = DataModel}) ->
    Reply = {ok, DataModel, KeyDef, ColumnsDef},
    {reply, Reply, State};
handle_call(delete_db, _From, State = #state{db_ref = DB,
					     options = Options,
					     name = Name,
					     path = Path,
					     subdir = Subdir}) ->
    ?debug("Deleting shard: ~p", [Name]),
    ok = delete_enterdb_ldb_resource(Name),
    ok = leveldb:close_db(DB),
    FullPath = filename:join([Path, Subdir, Name]),
    ok = leveldb:destroy_db(FullPath, Options),
    {stop, normal, ok, State#state{db_ref = undefined}};
handle_call(recreate_shard, _From, State = #state{db_ref = DB,
						  options = Options,
						  name = Name,
						  path = Path,
						  subdir = Subdir,
						  options_pl = OptionsPL}) ->
    ?debug("Recreating shard: ~p", [Name]),
    ok = delete_enterdb_ldb_resource(Name),
    ok = leveldb:close_db(DB),
    FullPath = filename:join([Path, Subdir, Name]),
    ok = leveldb:destroy_db(FullPath, Options), 
    
    %% Create new options. If table was re-opened, we cannot
    %% use the options including {create_if_missing, false}
    %% and {error_if_exists, false}
    
    IntOptionsPL = lists:keyreplace(error_if_exists, 1, OptionsPL,
				    {error_if_exists, true}),
    NewOptionsPL = lists:keyreplace(create_if_missing, 1, IntOptionsPL,
				    {create_if_missing, true}),
    OptionsRec = build_leveldb_options(NewOptionsPL),
    {ok, NewOptions} = leveldb:options(OptionsRec),
    {ok, NewDB} = leveldb:open_db(NewOptions, FullPath),
    ok = write_enterdb_ldb_resource(#enterdb_ldb_resource{name = Name,
							  resource = NewDB}),
    {reply, ok, State#state{db_ref = NewDB,
			    options = NewOptions,
			    is_empty = true,
			    options_pl = NewOptionsPL}};
handle_call({approximate_sizes, Ranges}, _From, #state{db_ref = DB} = State) ->
    R = leveldb:approximate_sizes(DB, Ranges),
    {reply, R, State};
handle_call(approximate_size, _From, State) ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    R = leveldb:approximate_size(DB, ReadOptions),
    {reply, R, State};
handle_call(get_iterator, _From, State) ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    R = leveldb:iterator(DB, ReadOptions),
    {reply, R, State};
handle_call(Req, From, State) ->
    R = ?warning("unkown request:~p, from: ~p, state: ~p", [Req, From, State]),
    {reply, R, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State = #state{db_ref = undefined}) ->
    ok;
terminate(_Reason, _State = #state{db_ref = DB,  name = Name}) ->
    ?debug("Terminating ldb worker for shard: ~p", [Name]),
    ok = delete_enterdb_ldb_resource(Name),
    ok = leveldb:close_db(DB).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Build a #leveldb_options{} record with provided proplist.
%% @end
%%--------------------------------------------------------------------
-spec build_leveldb_options(OptionsPL::[{atom(), term()}]) -> ok | {error, Reason::term()}.
build_leveldb_options(OptionsPL) ->
    leveldb_lib:build_leveldb_options(OptionsPL).

%%--------------------------------------------------------------------
%% @doc
%% Build a #leveldb_readoptions{} record with provided proplist.
%% @end
%%--------------------------------------------------------------------
-spec build_leveldb_readoptions(OptionsPL::[{atom(), term()}]) -> ok | {error, Reason::term()}.
build_leveldb_readoptions(OptionsPL) ->
    leveldb_lib:build_leveldb_readoptions(OptionsPL).

%%--------------------------------------------------------------------
%% @doc
%% Build a #leveldb_writeoptions{} record with provided proplist.
%% @end
%%--------------------------------------------------------------------
-spec build_leveldb_writeoptions(OptionsPL::[{atom(), term()}]) -> ok | {error, Reason::term()}.
build_leveldb_writeoptions(OptionsPL) ->
    leveldb_lib:build_leveldb_writeoptions(OptionsPL).

%%--------------------------------------------------------------------
%% @doc
%% Ensure the leveldb db is closed and there is no resource left
%% @end
%%--------------------------------------------------------------------
-spec ensure_closed(Name :: atom()) -> ok | {error, Reason :: term()}.
ensure_closed(Name) ->
    case read_enterdb_ldb_resource(Name) of
	undefined ->
	    ok;
	{ok, DB} ->
	    ?debug("Old DB resource found for shard: ~p, closing and deleting..", [Name]),
	    ok = delete_enterdb_ldb_resource(Name),
	    ok = leveldb:close_db(DB);
	{error, Reason} ->
	    {error, Reason}
    end.

 
%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_ldb_resource entry in mnesia ram_copy
%% @end
%%--------------------------------------------------------------------
-spec write_enterdb_ldb_resource(EnterdbLdbResource::#enterdb_ldb_resource{}) -> ok | {error, Reason::term()}.
write_enterdb_ldb_resource(EnterdbLdbResource) ->
    case enterdb_db:transaction(fun() -> mnesia:write(EnterdbLdbResource) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Read the #enterdb_ldb_resource entry in mnesia ram_copy
%% @end
%%--------------------------------------------------------------------
-spec read_enterdb_ldb_resource(Name :: atom()) -> {ok, Resource :: binary} |
						    {error, Reason::term()}.
read_enterdb_ldb_resource(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_ldb_resource, Name) end) of
        {atomic, []} ->
	    undefined;
	{atomic, [#enterdb_ldb_resource{resource = Resource}]} ->
            {ok, Resource};
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete the #enterdb_ldb_resource entry in mnesia ram_copy
%% @end
%%--------------------------------------------------------------------
-spec delete_enterdb_ldb_resource(Name :: atom()) -> ok | {error, Reason::term()}.
delete_enterdb_ldb_resource(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:delete(enterdb_ldb_resource, Name, write) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

-spec format_kvl(Type :: binary | term, KVL :: [kvp()],
		 KeyDef :: [atom()],
		 ColumnsDef ::[atom()],
		 DataModel :: atom()) -> [kvp()].
format_kvl(binary, KVL, _KeyDef, _ColumnsDef, _DataModel) ->
    KVL;
format_kvl(term, KVL, KeyDef, ColumnsDef, DataModel) ->
    [begin
	K = enterdb_lib:make_app_key(KeyDef, BK),
	V = enterdb_lib:make_app_value(DataModel, ColumnsDef, BV),
	{K, V}
     end || {BK, BV} <- KVL].
