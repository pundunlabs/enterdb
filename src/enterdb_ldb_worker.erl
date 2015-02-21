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
         delete/2]).

-define(SERVER, ?MODULE).

-include("enterdb.hrl").

-record(state, {db_ref,
                readoptions,
                writeoptions,
                name,
                key,
                columns,
                indexes,
                time_ordered,
                data_model,
                options,
                path}).

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
%%
%% @end
%%--------------------------------------------------------------------
-spec read(Shard::string(),
           Key::[{atom(), term()}]) -> {ok, Value:: term()} |
                                       {error, Reason::term()}.
read(Shard, Key) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {read, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Coulumns to given shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(Shard::string(),
            Key::[{atom(), term()}],
            Columns::[{atom(), term()}]) -> ok |{error, Reason::term()}.
write(Shard, Key, Columns) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {write, Key, Columns}).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard::string(),
             Key::[{atom(), term()}]) -> ok |
                                         {error, Reason::term()}.
delete(Shard, Key) ->
    ServerRef = list_to_atom(Shard),
    gen_server:call(ServerRef, {delete, Key}).


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
    OptionsPL = proplists:get_value(options, Args),
    
    TabRec = proplists:get_value(tab_rec, Args),
    #enterdb_table{path = Path,
                   key = Key,
                   columns = Columns,
                   indexes = Indexes,
                   options = TabOptions} = TabRec,

    TimeOrdered = proplists:get_value(time_ordered, TabOptions),
    DataModel = proplists:get_value(data_model, TabOptions),

    OptionsRec = build_leveldb_options(OptionsPL),

    case leveldb:options(OptionsRec) of
        {ok, Options} ->
            FullPath = filename:join(Path, Name),
            case leveldb:open_db(Options, FullPath) of
                {error, Reason} ->
                    {stop, {error, Reason}};
                {ok, DB} ->
                    ReadOptionsRec = build_leveldb_readoptions([]),
                    {ok, ReadOptions} = leveldb:readoptions(ReadOptionsRec),
                    WriteOptionsRec = build_leveldb_writeoptions([]),
                    {ok, WriteOptions} = leveldb:writeoptions(WriteOptionsRec),
                    {ok, #state{db_ref = DB,
                                readoptions = ReadOptions,
                                writeoptions = WriteOptions,
                                name = Name,
                                key = Key,
                                columns = Columns,
                                indexes = Indexes,
                                time_ordered = TimeOrdered,
                                data_model = DataModel,
                                options = OptionsRec,
                                path = Path}}
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
handle_call({read, RKey}, _From,
            State = #state{db_ref = DB,
                           readoptions = ReadOptions,
                           key = Key}) ->
    Reply = 
    case make_key(Key, RKey) of
        {ok, DbKey} ->
            {ok, BinData} = leveldb:get(DB, ReadOptions, DbKey),
            {ok, binary_to_term(BinData)};
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
handle_call({write, WKey, WColumns}, _From,
            State = #state{db_ref = DB,
                           writeoptions = WriteOptions,
                           key = Key,
                           columns = Columns,
                           indexes = Indexes,
                           time_ordered = TimeOrdered,
                           data_model = DataModel}) ->
    Reply = 
    case make_key(Key, WKey) of
        {ok, DbKey} ->
            case make_value(DataModel, Columns, WColumns) of
                {ok, DbValue} ->
                    case make_indexes(Indexes, Columns) of
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
handle_call({delete, RKey}, _From,
            State = #state{db_ref = DB,
                           writeoptions = WriteOptions,
                           key = Key}) ->
    Reply = 
    case make_key(Key, RKey) of
        {ok, DbKey} ->
            leveldb:delete(DB, WriteOptions, DbKey);
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
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
terminate(_Reason, _State = #state{db_ref = DB}) ->
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
 
