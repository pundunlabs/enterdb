%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%%
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
	 make_app_kvp/2,
	 recreate_shard/1]).

-define(SERVER, ?MODULE).

-include("enterdb.hrl").

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
		tab_name}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
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
%% Read range of keys from a given shard and return max Limit of
%% items in binary format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_binary(Shard :: string(),
			Range :: key_range(),
			Limit :: pos_integer()) -> {ok, [{binary(), binary()}]} |
						    {error, Reason :: term()}.
read_range_binary(Shard, Range, Limit) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read_range, Range, Limit, binary}).


%%--------------------------------------------------------------------
%% @doc
%% Read range of keys from a given shard and return max Limit of
%% items in erlang term format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_term(Shard :: string(),
		      Range :: key_range(),
		      Limit :: pos_integer()) -> {ok, [{binary(), binary()}]} |
						 {error, Reason :: term()}.
read_range_term(Shard, Range, Limit) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read_range, Range, Limit, term}).

%%--------------------------------------------------------------------
%% @doc
%% Format a key/value list or key/value pair of binaries
%% according to tables data model.
%% @end
%%--------------------------------------------------------------------
-spec make_app_kvp(Shard :: string(),
		   KVP :: {binary(), binary()} |
			  [{binary(), binary()}]) -> {ok, [{binary(), binary()}]} |
						     {error, Reason :: term()}.
make_app_kvp(Shard, KVP) ->
    ServerRef = list_to_atom(Shard),
    {ok, KeyDef, ColumnsDef} = gen_server:call(ServerRef, get_key_columns_def),
    AppKVP =
    case KVP of
	[_|_] ->
	    [{make_app_key(KeyDef, binary_to_term(BK)), binary_to_term(BV)} || {BK, BV} <- KVP];
	{BinKey, BinValue} ->
	    {make_app_key(KeyDef, binary_to_term(BinKey)), binary_to_term(BinValue)};
	[] ->
	    [];
	_ ->
	    {error, {invalid_arg, KVP}}
    end,
    {ok, AppKVP}.

%%--------------------------------------------------------------------
%% @doc
%% Recreate the shard by destroying the leveldb db and re-open a new one
%% @end
%%--------------------------------------------------------------------
-spec recreate_shard(Shard :: string()) -> ok.
recreate_shard(Shard) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, recreate_shard).

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
    ok = ensure_closed(Name),
 
    OptionsPL = proplists:get_value(options, Args),
    
    TabRec = proplists:get_value(tab_rec, Args),
    #enterdb_table{name = TabName,
		   path = Path,
                   key = Key,
                   columns = Columns,
                   indexes = Indexes,
                   options = TabOptions} = TabRec,

    TimeOrdered = proplists:get_value(time_ordered, TabOptions),
    Wrapped = proplists:get_value(wrapped, TabOptions, undefined),
    DataModel = proplists:get_value(data_model, TabOptions),

    OptionsRec = build_leveldb_options(OptionsPL),

    case leveldb:options(OptionsRec) of
        {ok, Options} ->
            FullPath = filename:join(Path, Name),
            case leveldb:open_db(Options, FullPath) of
                {error, Reason} ->
                    {stop, {error, Reason}};
                {ok, DB} ->
                    ok = write_enterdb_ldb_resource(#enterdb_ldb_resource{name = Name,
									  resource = DB}),
		    ReadOptionsRec = build_leveldb_readoptions([]),
                    {ok, ReadOptions} = leveldb:readoptions(ReadOptionsRec),
                    WriteOptionsRec = build_leveldb_writeoptions([]),
                    {ok, WriteOptions} = leveldb:writeoptions(WriteOptionsRec),
                    {ok, #state{db_ref = DB,
                                options = Options,
				readoptions = ReadOptions,
                                writeoptions = WriteOptions,
                                name = Name,
                                key = Key,
                                columns = Columns,
                                indexes = Indexes,
                                is_empty = true,
				time_ordered = TimeOrdered,
                                wrapped = Wrapped,
				data_model = DataModel,
                                path = Path,
				tab_name = TabName}}
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
handle_call({read, Key}, _From,
            State = #state{db_ref = DB,
                           readoptions = ReadOptions,
                           key = KeyDef}) ->
    Reply = 
    case make_key(KeyDef, Key) of
        {ok, DbKey} ->
	    case leveldb:get(DB, ReadOptions, DbKey) of
		{ok, BinData} ->
		    {ok, binary_to_term(BinData)};
		{error, Reason} ->
		    {error, Reason}
	    end;
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
handle_call({write, Key, Columns}, From,
	    State = #state{is_empty = true,
			   wrapped = {_, TimeMargin},
			   tab_name = TabName}) ->
    {ok, UpperLevelKey} = make_upper_level_key(Key, TimeMargin),
    recreate_upper_level(TabName, UpperLevelKey),
    handle_call({write, Key, Columns}, From,
		State#state{is_empty = false});
handle_call({write, Key, Columns}, _From,
            State = #state{db_ref = DB,
                           writeoptions = WriteOptions,
                           key = KeyDef,
                           columns = ColumnsDef,
                           indexes = IndexesDef,
                           time_ordered = TimeOrdered,
                           data_model = DataModel}) ->
    Reply = 
    case make_key(KeyDef, Key) of
        {ok, DbKey} ->
            case make_value(DataModel, ColumnsDef, Columns) of
                {ok, DbValue} ->
                    case make_indexes(IndexesDef, ColumnsDef) of
                        {ok, DbIndexes} ->
                            leveldb:put(DB, WriteOptions, DbKey, DbValue);
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
handle_call({delete, Key}, _From,
            State = #state{db_ref = DB,
                           writeoptions = WriteOptions,
                           key = KeyDef}) ->
    Reply = 
    case make_key(KeyDef, Key) of
        {ok, DbKey} ->
            leveldb:delete(DB, WriteOptions, DbKey);
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
handle_call({read_range, {StartKey, EndKey}, Limit, Type}, _From,
	     State = #state{db_ref = DB,
			    options = Options,
			    readoptions = ReadOptions,
			    key = KeyDef}) when Limit > 0 ->
    
    {ok, DbStartKey} = make_key(KeyDef, StartKey),
    {ok, DbEndKey} = make_key(KeyDef, EndKey),
    
    Reply =
    case leveldb:read_range(DB, Options, ReadOptions, {DbStartKey, DbEndKey}, Limit) of
	{ok, KVL} ->
	    case Type of
		binary ->
		    {ok, KVL};
		term ->
		    {ok, [ {make_app_key(KeyDef, binary_to_term(BK)), binary_to_term(BV)} || {BK, BV} <- KVL]}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end,
    {reply, Reply, State};
handle_call(get_key_columns_def, _From, State = #state{key = KeyDef, columns = ColumnsDef}) ->
    Reply = {ok, KeyDef, ColumnsDef},
    {reply, Reply, State};
handle_call(delete_db, _From, State = #state{db_ref= DB,
					     options = Options,
					     name = Name,
					     path = Path}) ->
    ok = leveldb:close_db(DB),
    ok = delete_enterdb_ldb_resource(Name),
    FullPath = filename:join(Path, Name),
    ok = leveldb:destroy_db(FullPath, Options),
    {stop, normal, ok, State#state{db_ref = undefined}};
handle_call(recreate_shard, _From, State = #state{db_ref= DB,
						  options = Options,
						  name = Name,
						  path = Path}) ->
    ok = leveldb:close_db(DB),
    ok = delete_enterdb_ldb_resource(Name),
    FullPath = filename:join(Path, Name),
    ok = leveldb:destroy_db(FullPath, Options), 
    {ok, NewDB} = leveldb:open_db(Options, FullPath),
    ok = write_enterdb_ldb_resource(#enterdb_ldb_resource{name = Name,
							  resource = NewDB}),
    {reply, ok, State#state{db_ref = NewDB,
			    is_empty = true}};
handle_call(_Request, _From, State) ->
    Reply = error_logger:warning_msg("unkown request:~p, from: ~p, gen_server state: ~p",
                                     [_Request, _From, State]),
    {reply, Reply, State}.

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
    ok = leveldb:close_db(DB),
    ok = delete_enterdb_ldb_resource(Name).

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
%% Make key according to table configuration and provided values.
%% @end
%%--------------------------------------------------------------------
-spec make_key(Key::[atom()],
               WKey::[{atom(), term()}]) -> {ok, DbKey::binary} |
                                            {error, Reason::term()}.
make_key(Key, WKey) ->
    KeyLen = length(Key),
    WKeyLen = length(WKey),
    if KeyLen == WKeyLen ->
	make_key(Key, WKey, []);
       true ->
        {error, "key_mismatch"}
    end.

-spec make_key(Key::[atom()],
               WKey::[{atom(), term()}],
               DBKetList::[term()]) -> ok | {error, Reason::term()}.
make_key([], _, DbKeyList) ->
    Tuple = list_to_tuple(lists:reverse(DbKeyList)),
    {ok, term_to_binary(Tuple)};
make_key([Field|Rest], WKey, DbKeyList) ->
    case lists:keyfind(Field, 1, WKey) of
        {_, Value} ->
            make_key(Rest, WKey, [Value|DbKeyList]);
        false ->
            {error, "key_mismatch"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Make app key according to table configuration and provided values.
%% @end
%%--------------------------------------------------------------------
-spec make_app_key(KeyDef::[atom()],
		   DbKey::tuple()) -> {ok, AppKey::key()} |
				      {error, Reason::term()}.
make_app_key(KeyDef, DbKey)->
    make_app_key(KeyDef, tuple_to_list(DbKey), []).

-spec make_app_key(KeyDef::[atom()],
                   DbKey::tuple(),
		   Acc :: [{atom, term()}]) -> {ok, AppKey::key()} |
		                               {error, Reason::term()}.
make_app_key([], [], Acc)->
    lists:reverse(Acc);
make_app_key([Field | KeyDef], [KeyVal | DbKey], Acc)->
    make_app_key(KeyDef, DbKey, [{Field, KeyVal} | Acc]).


%%--------------------------------------------------------------------
%% @doc
%% Make value according to table configuration and provided values.
%% @end
%%--------------------------------------------------------------------
-spec make_value(DataModel::data_model(),
                 Columns::[atom()],
                 WColumns::[{atom(), term()}])-> {ok, DbValue::binary()} |
                                                 {error, Reason::term()}.
make_value(binary, _, [Data]) ->
    {ok, term_to_binary(Data)};
make_value(binary, _, _) ->
    {error, "invalid_value"};
make_value(array, Columns, WColumns) ->
    make_array_value(Columns, WColumns);
make_value(hash, Columns, WColumns) ->
    make_hash_value(Columns, WColumns).

-spec make_array_value(Columns::[atom()],
                       WColumns::[{atom(), term()}]) -> 
                            {ok, DbValue::binary()} | 
                            {error, Reason::term()}.
make_array_value(Columns, WColumns) ->
    ColLen = length(Columns),
    WColLen = length(WColumns),
    if ColLen == WColLen ->
        make_array_value(Columns, WColumns, []);
       true ->
        {error, "column_mismatch"}
    end.

-spec make_array_value(Columns::[atom()],
                       WColumns::[{atom(), term()}],
                       DbValueList::[term()]) -> {ok, DbValue::binary()} |
                                                {error, Reason::term()}.
make_array_value([], _WColumns, DbValueList) ->
    Tuple = list_to_tuple(lists:reverse(DbValueList)),
    {ok, term_to_binary(Tuple)};
make_array_value([Field|Rest], WColumns, DbValueList) ->
    case lists:keyfind(Field, 1, WColumns) of
        {_, Value} ->
            make_array_value(Rest, WColumns, [Value|DbValueList]);
        false ->
            {error, "column_mismatch"}
    end.


-spec make_hash_value(Columns::[atom()],
                      WColumns::[{atom(), term()}]) -> 
                            {ok, DbValue::binary()} | 
                            {error, Reason::term()}.
make_hash_value(_Columns, WColumns) ->
    Map = maps:from_list(WColumns),
    {ok, term_to_binary(Map)}.

-spec make_indexes(Indexes::[atom()],
                   Columns::[atom()] ) -> {ok, DbIndexes::[{atom(),term()}]} |
                                          {error, Reason::term()}.
make_indexes([],_) ->
    {ok, []};
make_indexes(_, _)->
    {error, "not_supported_yet"}.


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
	    ok = leveldb:close_db(DB),
	    ok = delete_enterdb_ldb_resource(Name);
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

%%--------------------------------------------------------------------
%% @doc
%% Make a key with a timestamp value that is TimeMargin seconds larger
%% than the provided Key's timestamp
%% @end
%%--------------------------------------------------------------------
-spec make_upper_level_key(Key :: key(), TimeMargin :: pos_integer()) ->
    {ok, UpperLevelKey :: key()} |
    {error, Reason :: term()}.
make_upper_level_key(Key, TimeMargin) ->
    case lists:keyfind(ts, 1, Key) of
	{ts, {Macs, Secs, Mics}} ->
	    {ok, lists:keyreplace(ts, 1, Key, {ts, {Macs, Secs+TimeMargin, Mics}})};
	{ts, _ELSE} ->
	    {error, invalid_timestamp};
	false ->
	    {error, no_timestamp}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Make a key with a timestamp value that is TimeMargin seconds larger
%% than the provided Key's timestamp
%% @end
%%--------------------------------------------------------------------
-spec recreate_upper_level(TabName :: string(), UpperLevelKey:: key()) -> ok.
recreate_upper_level(TabName, UpperLevelKey) ->
    {ok, {level, Level}} = gb_hash:find_node(TabName, UpperLevelKey),
    {ok, Shards} = gb_hash:get_nodes(Level),
    [spawn(?MODULE, recreate_shard, [Shard]) || Shard <- Shards]. 
