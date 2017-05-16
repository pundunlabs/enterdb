%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2017 Pundun Labs AB
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
%% Module Description: RocksDB backend worker module
%% @end
%%  Created : 10 Apr 2017 by erdem <erdem@sitting>
%%%===================================================================

-module(enterdb_rdb_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([read/2,
         write/3,
         update/6,
         delete/2,
	 delete_db/1,
	 read_range_binary/3,
	 read_range_term/3,
	 read_range_n_binary/3,
	 recreate_shard/1,
	 approximate_sizes/2,
	 approximate_size/1,
	 get_iterator/2]).

%% OAM callbacks
-export([backup_db/2,
	 backup_db/3,
	 get_backup_info/1,
	 restore_db/1,
	 restore_db/2,
	 restore_db/3,
	 create_checkpoint/1]).

-define(SERVER, ?MODULE).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-record(state, {db_ref,
                options,
		readoptions,
                writeoptions,
                shard,
		db_path,
		wal_path,
		backup_path,
		checkpoint_path,
		ttl,
		options_pl,
		it_mon}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a server that manages an enterdb shard with rocksdb backend.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args :: [{atom(), term()} | atom()]) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {read, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Columns to given shard.
%% @end
%%--------------------------------------------------------------------
-spec write(Shard :: string(),
            Key :: key(),
            Columns :: [column()]) -> ok | {error, Reason :: term()}.
write(Shard, Key, Columns) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {write, Key, Columns}).

%%--------------------------------------------------------------------
%% @doc
%% Update Key according to Op on given shard.
%% @end
%%--------------------------------------------------------------------
-spec update(Shard :: string(),
             Key :: key(),
             Op :: update_op(),
	     DataModel :: data_model(),
	     Mapper :: module(),
	     Distributed :: boolean()) -> ok | {error, Reason :: term()}.
update(Shard, Key, Op, DataModel, Mapper, Dist) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {update, Key, Op, DataModel, Mapper, Dist}).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
             Key :: key()) -> ok | {error, Reason :: term()}.
delete(Shard, Key) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {delete, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Delete the database specified by given Shard.
%%
%% @end
%%--------------------------------------------------------------------
-spec delete_db(Args :: [term()]) ->
    ok | {error, Reason :: term()}.
delete_db(Args) ->
    Shard = maps:get(shard, Args),
    Subdir = maps:get(name, Args),
    Path = maps:get(db_path, Args),
    ok = ensure_closed(Shard),

    OptionsPL = maps:get(options, Args),
    {ok, Options} = rocksdb:options(OptionsPL),

    FullPath = filename:join([Path, Subdir, Shard]),
    rocksdb:destroy_db(FullPath, Options).

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
    ServerRef = enterdb_ns:get(Shard),
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
    ServerRef = enterdb_ns:get(Shard),
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
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {read_range_n, StartKey, N, binary}).

%%--------------------------------------------------------------------
%% @doc
%% Recreate the shard by destroying the rocksdb db and re-open a new
%% one
%% @end
%%--------------------------------------------------------------------
-spec recreate_shard(Shard :: string()) -> ok.
recreate_shard(Shard) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:cast(ServerRef, recreate_shard).

%%--------------------------------------------------------------------
%% @doc
%% Approximate the size of rocksdb db in bytes for given key ranges.
%% @end
%%--------------------------------------------------------------------
-spec approximate_sizes(Shard :: string(), Ranges :: key_range()) ->
    {ok, Sizes :: [pos_integer()]} | {error, Reason :: term()}.
approximate_sizes(Shard, Ranges) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, {approximate_sizes, Ranges}).

%%--------------------------------------------------------------------
%% @doc
%% Approximate the size of rocksdb db in bytes.
%% @end
%%--------------------------------------------------------------------
-spec approximate_size(Shard :: string()) ->
    {ok, Size :: pos_integer()} | {error, Reason :: term()}.
approximate_size(Shard) ->
    ServerRef = enterdb_ns:get(Shard),
    gen_server:call(ServerRef, approximate_size).

%%--------------------------------------------------------------------
%% @doc
%% Get an iterator resource from rocksdb backend.
%% @end
%%--------------------------------------------------------------------
-spec get_iterator(Caller :: pid(), Shard :: string()) ->
    {ok, It :: it()} | {error, Reason :: term()}.
get_iterator(Caller, Shard) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, {get_iterator, Caller}).

%%--------------------------------------------------------------------
%% @doc
%% Create a backup for the shard.
%% @end
%%--------------------------------------------------------------------
-spec backup_db(Shard :: string(), Timeout :: pos_integer()) ->
    ok | {error, Reason :: term()}.
backup_db(Shard, Timeout) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, backup_db, Timeout).

%%--------------------------------------------------------------------
%% @doc
%% Create a backup for the shard.
%% @end
%%--------------------------------------------------------------------
-spec backup_db(Shard :: string(), BackupDir :: string(), Timeout :: pos_integer()) ->
    ok | {error, Reason :: term()}.
backup_db(Shard, BackupDir, Timeout) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, {backup_db, BackupDir}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%% Retrieve previously taken backups' information..
%% @end
%%--------------------------------------------------------------------
-spec get_backup_info(Shard :: string()) ->
    {ok, map()} | {error, Reason :: term()}.
get_backup_info(Shard) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, get_backup_info).

%%--------------------------------------------------------------------
%% @doc
%% Restore shard from latest backup.
%% @end
%%--------------------------------------------------------------------
-spec restore_db(Shard :: string()) ->
    ok | {error, Reason :: term()}.
restore_db(Shard) ->
    restore_db(Shard, 0).

%%--------------------------------------------------------------------
%% @doc
%% Restore shard from given backup id.
%% @end
%%--------------------------------------------------------------------
-spec restore_db(Shard :: string(), BackupId :: integer()) ->
    ok | {error, Reason :: term()}.
restore_db(Shard, BackupId) when is_integer(BackupId) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, {restore_db, BackupId}).

%%--------------------------------------------------------------------
%% @doc
%% Restore shard from given backup id.
%% @end
%%--------------------------------------------------------------------
-spec restore_db(Shard :: string(), BackupId :: integer(), FromPath :: string()) ->
    ok | {error, Reason :: term()}.
restore_db(Shard, BackupId, FromPath) when is_integer(BackupId) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, {restore_db, BackupId, FromPath}).

%%--------------------------------------------------------------------
%% @doc
%% Create a backup for the shard.
%% @end
%%--------------------------------------------------------------------
-spec create_checkpoint(Shard :: string()) ->
    ok | {error, Reason :: term()}.
create_checkpoint(Shard) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, create_checkpoint).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: map()) ->
    {ok, State :: #state{}} |
    {ok, State :: #state{}, Timeout :: pos_integer() | infinity} |
    ignore |
    {stop, Reason :: term()}.
init(Args) ->
    Shard = maps:get(shard, Args),
    enterdb_ns:register_pid(self(), Shard),
    Subdir = maps:get(name, Args),
    ok = ensure_closed(Shard),
    OptionsPL = maps:get(options, Args),
    case rocksdb:options(OptionsPL) of
        {ok, Options} ->
            [DbPath, WalPath, BackupPath, CheckpointPath] =
		ensure_directories(Args, Subdir, Shard),
	    TTL = maps:get(ttl, Args, undefined),
	    case open_db(Options, DbPath, TTL) of
                {error, Reason} ->
                    {stop, {error, Reason}};
                {ok, DB} ->
		    ELR = #enterdb_ldb_resource{name = Shard, resource = DB},
                    ok = write_enterdb_ldb_resource(ELR),
		    %%ReadOptionsRec = build_readoptions([{tailing,true}]),
		    ReadOptionsRec = build_readoptions([]),
                    {ok, ReadOptions} = rocksdb:readoptions(ReadOptionsRec),
                    WriteOptionsRec = build_writeoptions([]),
                    {ok, WriteOptions} = rocksdb:writeoptions(WriteOptionsRec),
                    process_flag(trap_exit, true),
		    {ok, #state{db_ref = DB,
                                options = Options,
				readoptions = ReadOptions,
                                writeoptions = WriteOptions,
                                shard = Shard,
                                db_path = DbPath,
                                wal_path = WalPath,
                                backup_path = BackupPath,
                                checkpoint_path = CheckpointPath,
				ttl = TTL,
				options_pl = OptionsPL,
				it_mon = maps:new()}}
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
    Reply = rocksdb:get(DB, ReadOptions, DBKey),
    {reply, Reply, State};
handle_call({write, Key, Columns}, _From, State) ->
    #state{db_ref = DB,
           writeoptions = WriteOptions} = State,
    Reply = rocksdb:put(DB, WriteOptions, Key, Columns),
    {reply, Reply, State};
handle_call({update, DBKey, Op, DataModel, Mapper, Dist}, _From, State) ->
    #state{db_ref = DB,
	   readoptions = ReadOptions,
           writeoptions = WriteOptions} = State,
    BinValue =
	case rocksdb:get(DB, ReadOptions, DBKey) of
	    {ok, Value} -> Value;
	    _ -> make_empty_entry(DataModel)
	end,
    case enterdb_lib:apply_update_op(Op, BinValue, DataModel, Mapper, Dist) of
	{ok, BinValue} ->
	    {reply, {ok, BinValue}, State};
	{ok, Columns} ->
	    Reply =
		case rocksdb:put(DB, WriteOptions, DBKey, Columns) of
		    ok -> {ok, Columns};
		    Else -> Else
		end,
	    {reply, Reply, State}
    end;
handle_call({delete, DBKey}, _From, State) ->
    #state{db_ref = DB,
           writeoptions = WriteOptions} = State,
    Reply = rocksdb:delete(DB, WriteOptions, DBKey),
    {reply, Reply, State};
handle_call({read_range, Keys, Chunk, _Type}, _From, State) when Chunk > 0 ->
   #state{db_ref = DB,
	  options = Options,
	  readoptions = ReadOptions} = State,
    Reply = rocksdb:read_range(DB, Options, ReadOptions, Keys, Chunk),
    {reply, Reply, State};
handle_call({read_range_n, StartKey, N, _Type}, _From, State) when N >= 0 ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    Reply = rocksdb:read_range_n(DB, ReadOptions, StartKey, N),
    {reply, Reply, State};
handle_call({approximate_sizes, Ranges}, _From, #state{db_ref = DB} = State) ->
    R = rocksdb:approximate_sizes(DB, Ranges),
    {reply, R, State};
handle_call(approximate_size, _From, State) ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    R = rocksdb:approximate_size(DB, ReadOptions),
    {reply, R, State};
handle_call({get_iterator, Caller}, _From, State) ->
    #state{db_ref = DB,
	   readoptions = ReadOptions} = State,
    R = do_get_iterator(DB, ReadOptions),
    Mref = erlang:monitor(process, Caller),
    MonMap = State#state.it_mon,
    NewMonMap = maps:put(Mref, Caller, MonMap),
    {reply, R, State#state{it_mon = NewMonMap}};
handle_call({backup_db, BackupDir}, From,
            State = #state{db_ref = DB}) ->
    spawn(fun() ->
	    Reply = rocksdb:backup_db(DB, BackupDir),
	    gen_server:reply(From, Reply)
	  end ),
    {noreply, State};
handle_call(backup_db, From,
            State = #state{db_ref = DB,
                           backup_path = BackupPath}) ->
    spawn(fun() ->
	    Reply = rocksdb:backup_db(DB, BackupPath),
	    gen_server:reply(From, Reply)
	  end ),
    {noreply, State};
handle_call(get_backup_info, From,
            State = #state{backup_path = BackupPath}) ->
    spawn(fun() ->
	    Reply =
		case rocksdb:get_backup_info(BackupPath) of
		    {ok, List} when is_list(List) ->
			{ok, build_backup_info(List)};
		    Else -> Else
		end,
	    gen_server:reply(From, Reply)
	  end ),
    {noreply, State};
handle_call({restore_db, BackupId, FromPath}, _From,
            State = #state{shard = Shard,
			   db_ref = DB,
			   db_path = DbPath,
			   ttl = TTL,
			   options_pl = OptionsPL}) ->
    ok = delete_enterdb_ldb_resource(Shard),
    ok = rocksdb:close_db(DB),
    Reply =
	case BackupId of
	    0 -> rocksdb:restore_db(FromPath, DbPath, DbPath);
	    _ -> rocksdb:restore_db(FromPath, DbPath, DbPath, BackupId)
	end,
    IntOptionsPL = lists:keyreplace(error_if_exists, 1, OptionsPL,
				    {error_if_exists, false}),
    NewOptionsPL = lists:keyreplace(create_if_missing, 1, IntOptionsPL,
				    {create_if_missing, false}),
    {ok, NewOptions} = rocksdb:options(NewOptionsPL),
    {ok, NewDB} = open_db(NewOptions, DbPath, TTL),
    ELR = #enterdb_ldb_resource{name = Shard, resource = NewDB},
    ok = write_enterdb_ldb_resource(ELR),
    {reply, Reply, State#state{db_ref=NewDB,
			       options=NewOptions,
			       options_pl=NewOptionsPL}};
handle_call({restore_db, BackupId}, _From,
            State = #state{shard = Shard,
			   db_ref = DB,
			   db_path = DbPath,
			   backup_path = FromPath,
			   ttl = TTL,
			   options_pl = OptionsPL}) ->
    ?info("DbPath ~p", [DbPath]),
    ok = delete_enterdb_ldb_resource(Shard),
    ok = rocksdb:close_db(DB),
    Reply =
	case BackupId of
	    0 -> rocksdb:restore_db(FromPath, DbPath, DbPath);
	    _ -> rocksdb:restore_db(FromPath, DbPath, DbPath, BackupId)
	end,
    IntOptionsPL = lists:keyreplace(error_if_exists, 1, OptionsPL,
				    {error_if_exists, false}),
    NewOptionsPL = lists:keyreplace(create_if_missing, 1, IntOptionsPL,
				    {create_if_missing, false}),
    {ok, NewOptions} = rocksdb:options(NewOptionsPL),
    {ok, NewDB} = open_db(NewOptions, DbPath, TTL),
    ELR = #enterdb_ldb_resource{name = Shard, resource = NewDB},
    ok = write_enterdb_ldb_resource(ELR),
    {reply, Reply, State#state{db_ref=NewDB,
			       options=NewOptions,
			       options_pl=NewOptionsPL}};
handle_call(create_checkpoint, From,
            State = #state{db_ref = DB,
                           checkpoint_path = CheckpointPath}) ->
    spawn(fun() ->
	    del_dir(CheckpointPath),
	    Reply = rocksdb:create_checkpoint(DB, CheckpointPath),
	    gen_server:reply(From, Reply)
	  end ),
    {noreply, State};
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
handle_cast(recreate_shard, State = #state{shard = Shard,
					   db_ref = DB,
					   options = Options,
					   db_path = DbPath,
					   ttl = TTL,
					   options_pl = OptionsPL}) ->
    ?debug("Recreating shard: ~p", [Shard]),
    ok = delete_enterdb_ldb_resource(Shard),
    ok = rocksdb:close_db(DB),
    ok = rocksdb:destroy_db(DbPath, Options),

    %% Create new options. If table was re-opened, we cannot
    %% use the options including {create_if_missing, false}
    %% and {error_if_exists, false}

    IntOptionsPL = lists:keyreplace(error_if_exists, 1, OptionsPL,
				    {error_if_exists, true}),
    NewOptionsPL = lists:keyreplace(create_if_missing, 1, IntOptionsPL,
				    {create_if_missing, true}),
    {ok, NewOptions} = rocksdb:options(NewOptionsPL),
    {ok, NewDB} = open_db(NewOptions, DbPath, TTL),
    ok = write_enterdb_ldb_resource(#enterdb_ldb_resource{name = Shard,
							  resource = NewDB}),
    {noreply, State#state{db_ref = NewDB,
			  options = NewOptions,
			  options_pl = NewOptionsPL}};
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
handle_info({'DOWN', Mref, process, Pid, _Info},
	    State = #state{it_mon = MonMap}) ->
    NewMonMap = maps:remove(Mref, MonMap),
    {noreply, State#state{it_mon = NewMonMap}};
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
terminate(_Reason, _State = #state{shard = Shard,
				   it_mon = MonMap}) ->
    ?debug("Terminating ldb worker for shard: ~p", [Shard]),
    maps:fold(fun(Mref, Pid, _Acc) ->
		erlang:demonitor(Mref, [flush]),
		Res = (catch gen_server:stop(Pid)),
		?debug("Stop iterator: ~p", [Res])
	      end, ok, MonMap),
    ensure_closed(Shard).

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
-spec ensure_directories(Args :: map(),
			 Subdir :: string(),
			 Shard :: string()) ->
    term().
ensure_directories(_Args, Subdir, Shard) ->
    [ begin
	Dir = enterdb_lib:get_path(P),
	Path = filename:join([Dir, Subdir, Shard]),
	filelib:ensure_dir(Path),
	Path
      end || P <- [db_path, wal_path, backup_path, checkpoint_path]].

-spec open_db(Options :: term(),
	      DbPath :: string(),
	      TTL :: undefined | pos_integer()) ->
    {ok, DB :: term()} | {error, Reason :: term()}.
open_db(Options, DbPath, undefined) ->
    rocksdb:open_db(Options, DbPath);
open_db(Options, DbPath, TTL) ->
    rocksdb:open_db_with_ttl(Options, DbPath, TTL).

%%--------------------------------------------------------------------
%% @doc
%% Get a rocksdb iterator and register the oterator resource to
%% keep track.
%% @end
%%--------------------------------------------------------------------
-spec do_get_iterator(DB :: binary(),
		      ReadOptions :: binary()) ->
    {ok, It :: binary()} | {error, Reason :: term()}.
do_get_iterator(DB, ReadOptions) ->
    case rocksdb:iterator(DB, ReadOptions) of
	{ok, It} ->
	    {ok, It};
	{error, R} ->
	    {error, R}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Build a #rocksdb_readoptions{} record with provided proplist.
%% @end
%%--------------------------------------------------------------------
-spec build_readoptions(OptionsPL :: [{atom(), term()}]) ->
    ok | {error, Reason :: term()}.
build_readoptions(OptionsPL) ->
    rocksdb_lib:build_readoptions(OptionsPL).

%%--------------------------------------------------------------------
%% @doc
%% Build a #rocksdb_writeoptions{} record with provided proplist.
%% @end
%%--------------------------------------------------------------------
-spec build_writeoptions(OptionsPL :: [{atom(), term()}]) ->
    ok | {error, Reason::term()}.
build_writeoptions(OptionsPL) ->
    rocksdb_lib:build_writeoptions(OptionsPL).

%%--------------------------------------------------------------------
%% @doc
%% Ensure the rocksdb db is closed and there is no resource left
%% @end
%%--------------------------------------------------------------------
-spec ensure_closed(Shard :: string()) ->
    ok | {error, Reason :: term()}.
ensure_closed(Shard) ->
    case read_enterdb_ldb_resource(Shard) of
	undefined ->
	    ok;
	{ok, DB} ->
	    ?debug("Old DB resource found for shard: ~p, closing and deleting..", [Shard]),
	    ok = delete_enterdb_ldb_resource(Shard),
	    ok = rocksdb:close_db(DB);
	{error, Reason} ->
	    {error, Reason}
    end.

 
%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_ldb_resource entry in mnesia ram_copy
%% @end
%%--------------------------------------------------------------------
-spec write_enterdb_ldb_resource(EnterdbLdbResource :: #enterdb_ldb_resource{}) ->
    ok | {error, Reason :: term()}.
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
-spec read_enterdb_ldb_resource(Shard :: string()) ->
    {ok, Resource :: binary} | {error, Reason :: term()}.
read_enterdb_ldb_resource(Shard) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_ldb_resource, Shard) end) of
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
-spec delete_enterdb_ldb_resource(Shard :: string()) ->
    ok | {error, Reason :: term()}.
delete_enterdb_ldb_resource(Shard) ->
    case enterdb_db:transaction(fun() -> mnesia:delete(enterdb_ldb_resource, Shard, write) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

make_empty_entry(map) ->
    <<131,106>>;
make_empty_entry(array) ->
    <<131,104,0>>;
make_empty_entry(kv)->
    <<>>.

build_backup_info(List) ->
    build_backup_info(List, []).

build_backup_info([], Acc) ->
    lists:reverse(Acc);
build_backup_info([{Id, Ts, Size, NumFiles, Metadata} | Rest], Acc) ->
    Map = maps:from_list([{backup_id, Id},
			  {timestamp, Ts},
			  {size, Size},
			  {number_of_files, NumFiles},
			  {application_metadata, Metadata}]),
    build_backup_info(Rest, [Map | Acc]).

del_dir(Dir) ->
    case file:list_dir_all(Dir) of
	{ok, Filenames} -> del_dir(Dir, Filenames);
	E -> E
    end.

del_dir(Dir, []) ->
    file:del_dir(Dir);
del_dir(Dir, [File | Rest]) ->
    file:delete(filename:join([Dir, File])),
    del_dir(Dir, Rest).
