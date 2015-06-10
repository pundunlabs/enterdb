%%%------------------------------------------------------------%%% @author erdem aksu <erdem@sitting>
%%% @copyright (C) 2015, Mobile Arts AB
%%% @doc
%%% Enterdb the key/value storage.
%%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_lib).

%% API
-export([verify_create_table_args/1,
         get_shards/3,
         create_table/1,
         open_leveldb_db/1,
	 close_leveldb_db/1,
	 delete_leveldb_db/1,
	 read_range/3,
	 read_range_n/3,
	 approximate_size/2]).

-export([make_db_key/2,
	 make_key/2,
	 make_key_columns/3,
	 make_db_value/3,
	 make_db_indexes/2,
	 make_app_key/2,
	 make_app_value/2,
	 make_app_value/3,
	 make_app_kvp/4]).

-export([do_create_shard/2,
         do_open_shard/2,
	 open_shard/1,
	 create_leveldb_shard/2,
	 open_leveldb_shard/2,
	 close_leveldb_shard/1,
	 write_enterdb_table/1,
	 get_details/1,
	 get_table_options/1]).

-include("enterdb.hrl").
-include("gb_log.hrl").

%%%===================================================================
%%% API
%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Verify the args given to enterdb:create_table/5
%% @end
%%--------------------------------------------------------------------
-spec verify_create_table_args(Args :: [{atom(), term()}]) -> {ok, #enterdb_table{}} |
                                                              {error, Reason::term()}.
verify_create_table_args(Args)->
    verify_create_table_args(Args, #enterdb_table{}).

verify_create_table_args([], #enterdb_table{} = EnterdbTable)->
    {ok, EnterdbTable};
verify_create_table_args([{name, Name}|Rest], #enterdb_table{} = EnterdbTable) when is_list(Name)->
    case verify_name(Name) of
        ok ->
            verify_create_table_args(Rest, EnterdbTable#enterdb_table{name = Name});            
        {error, Reason} ->
            {error, Reason}
    end;
verify_create_table_args([{key, Key}|Rest], #enterdb_table{} = EnterdbTable) when is_list(Key)->
    case verify_fields(Key) of
        ok ->
           verify_create_table_args(Rest, EnterdbTable#enterdb_table{key = Key});
        {error, Reason} ->
           {error, {Key, Reason}}
    end;
verify_create_table_args([{columns, Columns}|Rest],
                          #enterdb_table{key = Key} = EnterdbTable) when is_list(Columns)->
    case verify_fields(Columns) of
        ok ->
           OnlyDataColumns = lists:subtract(Columns, Key),
           verify_create_table_args(Rest, EnterdbTable#enterdb_table{columns = OnlyDataColumns});
        {error, Reason} ->
           {error, {Columns, Reason}}
    end;
verify_create_table_args([{indexes, Indexes}|Rest],
                         #enterdb_table{key = Key,
                                        columns = Columns} = EnterdbTable) when is_list(Indexes)->
    case verify_fields(Indexes++Key) of
        ok ->
            {ok, NewColumns} = add_index_fields_to_columns(Indexes, Columns),
            verify_create_table_args(Rest, EnterdbTable#enterdb_table{columns = NewColumns,
                                                                      indexes = Indexes});
        {error, Reason} ->
           {error, {Indexes, Reason}}
    end;
verify_create_table_args([{options, Options}|Rest],
                         #enterdb_table{} = EnterdbTable) when is_list(Options)->
    case verify_table_options(Options) of
        ok ->
            verify_create_table_args(Rest, EnterdbTable#enterdb_table{options = Options});
        {error, Reason} ->
            {error, Reason}
    end;
verify_create_table_args([{Arg, _}|_], _)->
    {error, {Arg, "not_list"}}.

%%-------------------------------------------------------------------
%% @doc
%% Verify if the given list elements are all atoms and the list
%% has unique elements
%% @end
%%-------------------------------------------------------------------
-spec verify_fields(List::[term()]) -> ok | {error, Reason::term()}.
verify_fields([])->
    ok;
verify_fields([Elem | Rest]) when is_atom(Elem)->
    case lists:member(Elem, Rest) of
        true ->
            {error, "dublicate_key"};
        false ->
            verify_fields(Rest)
    end;
verify_fields(_) ->
    {error, "not_atom"}.

-spec verify_name(String::string())-> ok | {error, Reason::term()}.
verify_name(Name) ->
    case verify_name(Name, 0) of
        ok -> check_if_table_exists(Name);
        Error -> Error
    end.

-spec verify_name(String::string(), Acc::non_neg_integer())-> ok | {error, Reason::term()}.
verify_name(_, Acc) when Acc > ?MAX_TABLE_NAME_LENGTH ->
    {error, "too_long_name"};
verify_name([Char|_Rest], _Acc) when Char > 255 ->
    {error, "non_unicode_name"};
verify_name([_Char|Rest], Acc) ->
    verify_name(Rest, Acc+1);
verify_name([], _) ->
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Get details about a shard
%% @end
%%-------------------------------------------------------------------
-spec get_details(string()) -> #enterdb_stab{} | {error, Reason::term()}.
get_details(Shard) ->
    case mnesia:dirty_read(enterdb_stab, Shard) of
	[ShardTab] ->
	    ShardTab;
	_ ->
	    {error, no_such_table}
    end.

-spec add_index_fields_to_columns(Indexes::[atom()], Columns::[atom()])-> {ok, NewColumns::[atom()]}.
add_index_fields_to_columns([], Columns)->
    {ok, Columns};
add_index_fields_to_columns([Elem|Rest], Columns)->
    case lists:member(Elem, Columns) of
        true ->
            add_index_fields_to_columns(Rest, Columns);
        fasle ->
            add_index_fields_to_columns(Rest, Columns++[Elem])
    end.

-spec verify_table_options(Options::[table_option()]) -> ok | {error, Reason::term()}.
verify_table_options([{time_ordered, Bool}|Rest])
when is_boolean(Bool) ->
    verify_table_options(Rest);

%% Nodes
verify_table_options([{nodes, Nodes}|Rest])
when is_list(Nodes) ->
    verify_table_options(Rest);

%% Number of Shards
verify_table_options([{shards, NumOfShards}|Rest])
when is_integer(NumOfShards), NumOfShards > 0 ->
    verify_table_options(Rest);

%% Table types
verify_table_options([{type, Type}|Rest])
    when
	 Type =:= leveldb;
         Type =:= ets_leveldb;
	 Type =:= leveldb_wrapped;
	 Type =:= ets_levedb_wrapped
    ->
	verify_table_options(Rest);

%% Data Model
verify_table_options([{data_model, DM}|Rest])
    when
	DM == binary;
        DM == array;
        DM == hash
    ->
	verify_table_options(Rest);

%% Wrapping details for leveldb parts
verify_table_options([{wrapped, {FileMargin, TimeMargin}}|Rest])
    when
	is_integer( FileMargin ), FileMargin > 0,
	is_integer( TimeMargin ), TimeMargin > 0
    ->
	verify_table_options(Rest);

%% wrapping details for ets part of wrapped db
verify_table_options([{mem_wrapped, {BucketSpan, NumBuckets}}|Rest])
    when
	is_integer( BucketSpan ), BucketSpan > 0,
	is_integer( NumBuckets ), NumBuckets > 0
    ->
	verify_table_options(Rest);
%% Bad Option
verify_table_options([Elem|_])->
    {error, {Elem, "invalid_option"}};
%% All Options OK
verify_table_options([]) ->
    ok.

-spec check_if_table_exists(Name::string()) -> ok | {error, Reason::term()}.
check_if_table_exists(Name)->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_table, Name) end) of
        {atomic, []} ->
            ok;
        {atomic, [_Table]} ->
            {error, "table_exists"};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get tables options based on shared name
%% @end
%%--------------------------------------------------------------------
get_table_options(Shard) ->
    TD = get_details(Shard),
    case mnesia:dirty_read(enterdb_table, TD#enterdb_stab.name) of
	[#enterdb_table{options = Options}] ->
	    {ok, Options};
	_ ->
	    {error, no_table}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Allocate nodes to shard and return list of {node(), Shard}
%% @end
%%--------------------------------------------------------------------
allocate_nodes(Nodes, Shards) ->
    allocate_node(Nodes, Shards, 1, []).
%% helper function to allocate node to shard
allocate_node(Nodes, [Shard | R], N, Aux) ->
    NShard = {lists:nth(N, Nodes), Shard},
    allocate_node(Nodes, R, N+1 rem (length(Nodes)), [NShard | Aux]);
allocate_node(_, [], _N, Aux) ->
    lists:reverse(Aux).
%%--------------------------------------------------------------------
%% @doc
%% Create and return list of #enterdb_shard records for all shards
%% @end
%%--------------------------------------------------------------------
-spec get_shards(Name :: string(),
		 NumOfShards :: pos_integer(),
		 Nodes :: [node()]) ->  {ok, [{node(), string()}]}.
get_shards(Name, NumOfShards, Nodes) ->
    Shards = [lists:concat([Name,"_shard",N]) ||N <- lists:seq(0, NumOfShards-1)],
    Options = [{algorithm, sha}, {strategy, uniform}],
    AllocatedShards = allocate_nodes(Nodes, Shards),
    %% TODO: move creation of ring to application also handling distribution
    gb_hash:create_ring(Name, AllocatedShards, Options),
    {ok, AllocatedShards}.

%%--------------------------------------------------------------------
%% @doc
%% Call create table (shard) for each shard
%% @end
%%--------------------------------------------------------------------
-spec create_table(EnterdbTable::#enterdb_table{}) -> ok | {error, Reason::term()}. 
create_table(#enterdb_table{shards = Shards} = EnterdbTable)->
     ShardRes =
	[ rpc:call(Node, ?MODULE, do_create_shard, [Shard, EnterdbTable])
	    || {Node, Shard} <- Shards ],
    case lists:usort(ShardRes) of
	[ok] -> %% all shards was created, lets write enterdb_table
	    SchemaRes =
		[ rpc:call(Node, ?MODULE, write_enterdb_table, [EnterdbTable])
		    || Node <- lists:usort([Node || {Node, _} <- Shards])],
		%% TODO: do error handling (rollback and such)
		lists:usort(SchemaRes);

	[R] -> %% all shards replied the same
	    R;
	Error ->
	    {error, Error}
    end.

do_create_shard(Shard, EDBT) ->
    Options = EDBT#enterdb_table.options,
    DataModel = proplists:get_value(data_model, Options),
    ESTAB =
	#enterdb_stab{shard = Shard,
		      name = EDBT#enterdb_table.name,
		      type = EDBT#enterdb_table.type,
		      key  = EDBT#enterdb_table.key,
		      columns = EDBT#enterdb_table.columns,
		      indexes = EDBT#enterdb_table.indexes,
		      comparator = EDBT#enterdb_table.comparator,
		      data_model = DataModel},
    do_create_shard_type(Shard, ESTAB),
    write_shard_table(ESTAB).

do_create_shard_type(Shard, #enterdb_stab{type = Type} = EDBT)
					when Type =:= leveldb ->
    create_leveldb_shard(Shard, EDBT);

do_create_shard_type(Shard, #enterdb_stab{type = Type} = EDBT)
					when Type =:= ets_leveldb ->
    %% TODO: init LRU-Cache here as well
    create_leveldb_shard(Shard, EDBT);

do_create_shard_type(Shard, #enterdb_stab{type = Type} = EDBT)
					when Type =:= ets_leveldb_wrapped ->
    Res = enterdb_mem:init_tab(Shard, EDBT),
    ?debug("enterdb_mem:init_tab returned ~p", [Res]),
    create_leveldb_shard(Shard, EDBT).

%%--------------------------------------------------------------------
%% @doc
%% Open an existing enterdb database shard.
%% @end
%%--------------------------------------------------------------------
-spec open_shard(Name :: string())-> ok | {error, Reason :: term()}.
open_shard(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_stab, Name) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [ShardTab]} ->
	     do_open_shard(Name, ShardTab);
	{error, Reason} ->
            {error, Reason}
    end.

%% Open existing shard locally
do_open_shard(Shard, #enterdb_stab{type = Type} = EDBT)
				      when Type =:= leveldb ->
    open_leveldb_shard(Shard, EDBT);

do_open_shard(Shard, #enterdb_stab{type = Type} = EDBT)
					when Type =:= ets_leveldb ->
    %% TODO: init LRU-Cache here as well
    open_leveldb_shard(Shard, EDBT);

do_open_shard(Shard, #enterdb_stab{type = Type} = EDBT)
					when Type =:= ets_leveldb_wrapped ->
    Res = enterdb_mem:init_tab(Shard, EDBT),
    ?debug("enterdb_mem:init_tab returned ~p", [Res]),
    open_leveldb_shard(Shard, EDBT).

%% create leveldb shard
%% TODO: move out to levedb specific lib.
create_leveldb_shard(Shard, EDBT) ->
    Options = [{comparator, EDBT#enterdb_stab.comparator},
	       {create_if_missing, true},
	       {error_if_exists, true}],
    ChildArgs = [{name, Shard}, {subdir, EDBT#enterdb_stab.name},
                 {options, Options}, {tab_rec, EDBT}],
    {ok, _Pid} = supervisor:start_child(enterdb_ldb_sup, [ChildArgs]),
    ok.

%% open leveldb shard
%% TODO: move out to levedb specific lib.
open_leveldb_shard(Shard, EDBT) ->
    Options = [{comparator, EDBT#enterdb_stab.comparator},
	       {create_if_missing, false},
	       {error_if_exists, false}],
    ChildArgs = [{name, Shard}, {subdir, EDBT#enterdb_stab.name},
                 {options, Options}, {tab_rec, EDBT}],
    {ok, _Pid} = supervisor:start_child(enterdb_ldb_sup, [ChildArgs]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_table entry in mnesia disc_copy
%% @end
%%--------------------------------------------------------------------
-spec write_shard_table(EnterdbShard::#enterdb_stab{}) -> ok | {error, Reason :: term()}.
write_shard_table(EnterdbShard) ->
    case enterdb_db:transaction(fun() -> mnesia:write(EnterdbShard) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end. 
%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_table entry in mnesia disc_copy
%% @end
%%--------------------------------------------------------------------
-spec write_enterdb_table(EnterdbTable::#enterdb_table{}) -> ok | {error, Reason :: term()}.
write_enterdb_table(EnterdbTable) ->
    case enterdb_db:transaction(fun() -> mnesia:write(EnterdbTable) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Open an existing leveldb database specified by #enterdb_table{}.
%% @end
%%--------------------------------------------------------------------
-spec open_leveldb_db(Table :: #enterdb_table{})-> ok | {error, Reason :: term()}.
open_leveldb_db(EDBT = #enterdb_table{comparator = Comp,
				      shards = Shards}) ->
    Options = [{comparator, Comp},
	       {create_if_missing, false},
               {error_if_exists, false}],
    open_leveldb_db(Options, EDBT, Shards).

-spec open_leveldb_db(Options :: [{atom(),term()}],
                      EDBT :: #enterdb_table{},
                      Shards :: [#enterdb_shard{}]) ->
    ok | {error, Reason :: term()}.
open_leveldb_db(_Options, _EDBT, []) ->
    ok;
open_leveldb_db(Options, EDBT,
		[#enterdb_shard{name = ShardName,
				subdir = Subdir} | Rest]) ->
     ChildArgs = [{name, ShardName}, {subdir, Subdir},
                  {options, Options}, {tab_rec, EDBT}],
     case supervisor:start_child(enterdb_ldb_sup, [ChildArgs]) of
        {ok, _Pid} ->
            open_leveldb_db(Options, EDBT, Rest);
        {error, Reason} ->
            {error, Reason}
     end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing leveldb database specified by #enterdb_table{}.
%% @end
%%--------------------------------------------------------------------
-spec close_leveldb_db(Table :: #enterdb_table{})-> ok | {error, Reason :: term()}.
close_leveldb_db(EDBT = #enterdb_table{shards = Shards}) ->
    close_leveldb_db(EDBT, Shards).

-spec close_leveldb_db(EDBT :: #enterdb_table{},
                       Shards :: [#enterdb_shard{}]) ->
    ok | {error, Reason :: term()}.
close_leveldb_db(_EDBT, []) ->
    ok;
close_leveldb_db(EDBT, [#enterdb_shard{name = Name} | Rest]) ->
    %% Terminating simple_one_for_one child requires OTP_REL >= R14B03
    case supervisor:terminate_child(enterdb_ldb_sup, list_to_atom(Name)) of
	ok ->
	    close_leveldb_db(EDBT, Rest);
	{error, Reason} ->
	    {error, Reason}
     end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing leveldb shard specified by name.
%% @end
%%--------------------------------------------------------------------
-spec close_leveldb_shard(Shard :: string())-> ok | {error, Reason :: term()}.
close_leveldb_shard(Shard) ->
    %% Terminating simple_one_for_one child requires OTP_REL >= R14B03
    supervisor:terminate_child(enterdb_ldb_sup, list_to_atom(Shard)).
%%--------------------------------------------------------------------
%% @doc
%% Delete an existing leveldb database specified by #enterdb_table{}.
%% This function should be called within a mnesia transaction.
%% @end
%%--------------------------------------------------------------------
-spec delete_leveldb_db(Table :: #enterdb_table{})-> ok | {error, Reason :: term()}.
delete_leveldb_db(#enterdb_table{name = Name, shards = Shards}) ->
    ok = delete_leveldb_db_shards(Shards),
    ok = delete_hash_ring(Name),
    mnesia:delete(enterdb_table, Name, write).

-spec delete_leveldb_db_shards(Shards :: [#enterdb_shard{}]) ->
    ok | {error, Reason :: term()}.
delete_leveldb_db_shards([]) ->
    ok;
delete_leveldb_db_shards([#enterdb_shard{name = Name} | Rest]) ->
    ok = enterdb_ldb_worker:delete_db(Name),
    delete_leveldb_db_shards(Rest).

%%--------------------------------------------------------------------
%% @doc
%% Delete the compiled and stored hash rings for given table by Name.
%% @end
%%--------------------------------------------------------------------
-spec delete_hash_ring(Name :: string())->
    ok | {error, Reason :: term()}.
delete_hash_ring(Name) ->
    case gb_hash:get_nodes(Name) of
	undefined ->
	    {error, "no_table"};
	{ok, {level, Levels}} ->
	    [ ok = gb_hash:delete_ring(L) || L <- Levels ],
	    ok = gb_hash:delete_ring(Name);
	{ok, _Shards} ->
	    ok = gb_hash:delete_ring(Name)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads a Range of Keys from table with name Name and returns max
%% Chunk items.
%% @end
%%--------------------------------------------------------------------
-spec read_range(Name :: string(),
		 Range :: key_range(),
		 Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} |
    {error, Reason::term()}.
read_range(Name, Range, Chunk) ->
    case gb_hash:get_nodes(Name) of
	undefined ->
	    {error, "no_table"};
	{ok, Shards} ->
	    read_range_on_shards(Name, Shards, Range, Chunk)
    end.


-spec read_range_on_shards(Name :: string(),
			   Shards :: [string()],
			   Range :: key_range(),
			   Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} | {error, Reason :: term()}.
read_range_on_shards(Name, Shards, Range, Chunk)->
    Args = enterdb:table_info(Name,[data_model,key,columns,comparator]),
    KeyDef = proplists:get_value(key, Args),
    {StartKey, StopKey} = Range,
    {ok, StartKeyDB} = make_db_key(KeyDef, StartKey),
    {ok, StopKeyDB}  = make_db_key(KeyDef, StopKey),
    RangeDB = {StartKeyDB, StopKeyDB},
    KVLs_and_Conts =
	[begin
	    {ok, KVL, Cont} =
		rpc:call(Node,enterdb_ldb_worker,read_range_binary,[Shard, RangeDB, Chunk]),
	    {KVL, Cont}
	 end || {Node, Shard} <- Shards],
    {KVLs, Conts} = lists:unzip(KVLs_and_Conts),

    DataModel = proplists:get_value(data_model, Args),
    ColumnsDef = proplists:get_value(columns, Args),
    Comparator = proplists:get_value(comparator, Args),

    ContKeys = [K || K <- Conts, K =/= complete],
    {ok, KVL, ContKey} = merge_and_cut_kvls(KeyDef, Comparator,
					    KVLs, ContKeys),

    {ok, ResultKVL} = make_app_kvp(DataModel, KeyDef, ColumnsDef, KVL),
    {ok, ResultKVL, ContKey}.

-spec merge_and_cut_kvls(KeyDef :: [atom()],
			 Comparator :: comparator(),
			 KVLs :: [[kvp()]],
			 ContKeys :: [binary()]) ->
    {ok, KVL :: [kvp()]}.
merge_and_cut_kvls(_KeyDef, _Comparator, KVLs, []) ->
   {ok, KVL} = leveldb_utils:merge_sorted_kvls( KVLs ),
   {ok, KVL, complete};
merge_and_cut_kvls(KeyDef, Comparator, KVLs, ContKeys) ->
    {Cont, _} = ContKVP = reduce_cont(Comparator, ContKeys),
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls( [[ContKVP]|KVLs] ),
    ContKey =  make_app_key(KeyDef, Cont),
    {ok, cut_kvl_at(Cont, MergedKVL), ContKey}.

-spec reduce_cont(Comparator :: comparator(),
		  Conts :: [binary()]) -> key().
reduce_cont(Comparator, ContKeys) ->
    Dir = comparator_to_dir(Comparator),
    SortableKVPs = [{K, <<>>} || K <- ContKeys],
    {ok, Sorted} = leveldb_utils:sort_kvl( Dir, SortableKVPs ),
    hd(Sorted).

-spec cut_kvl_at(Cont :: binary(), KVL :: [kvp()]) ->
    CutKVL :: [kvp()].
cut_kvl_at(Bin, KVL) ->
    cut_kvl_at(Bin, KVL, []).

-spec cut_kvl_at(Cont :: binary(), KVL :: [kvp()], Acc :: [kvp()]) ->
    CutKVL :: [kvp()].
cut_kvl_at(_Bin, [], Acc) ->
    lists:reverse(Acc);
cut_kvl_at(Bin, [{Bin, _} | _], Acc) ->
    lists:reverse(Acc);
cut_kvl_at(Bin, [KVP | Rest], Acc) ->
    cut_kvl_at(Bin, Rest, [KVP | Acc]).

-spec get_level_range(StartLevel :: string(),
		      EndLevel :: string(),
		      Levels :: [string()]) -> LevelRange :: [string()].
get_level_range(StartLevel, StartLevel, _) ->
    [StartLevel];
get_level_range(StartLevel, EndLevel, [EndLevel | Levels]) ->
    get_level_range_acc(StartLevel, Levels, [EndLevel]);
get_level_range(StartLevel, EndLevel, [AnyLevel | Levels]) ->
     get_level_range(StartLevel, EndLevel, lists:append(Levels, [AnyLevel])). 

-spec get_level_range_acc(StartLevel :: string(),
			  Levels :: [string()], Acc :: [string()]) -> Acc :: [string()].  
get_level_range_acc(StartLevel, [StartLevel | _Levels], Acc) ->
    [StartLevel|Acc];
get_level_range_acc(StartLevel, [AnyLevel | Levels], Acc) ->
    get_level_range_acc(StartLevel, Levels, [AnyLevel | Acc]).

%%--------------------------------------------------------------------
%%]c
%% Reads a N number of Keys from table with name Name starting from
%% StartKey and returns.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n(Name :: string(),
		   StartKey :: key(),
		   N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason::term()}.
read_range_n(Name, StartKey, N) ->
    case gb_hash:get_nodes(Name) of
	undefined ->
	    {error, "no_table"};
	{ok, Shards} ->
	    read_range_n_on_shards(Name, Shards, StartKey, N)
    end.

-spec read_range_n_on_levels(Name :: string(),
			     Levels :: [string()],
			     StartKey :: key(),
			     N :: pos_integer(),
			     Acc :: [kvp()]) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_levels(_Name, [], _StartKey, _N, Acc) ->
    {ok, Acc};
read_range_n_on_levels(Name, [Level|Rest], StartKey, N, Acc) ->
    {ok, Shards} = gb_hash:get_nodes(Level),
    {ok, KVL} = read_range_n_on_shards(Name, Shards, StartKey, N),
    Len = length(KVL),
    if Len =< N ->
	    read_range_n_on_levels(Name, Rest, StartKey, N-Len,
				   lists:append(Acc, KVL));
	true ->
	    AddKVL = lists:sublist(KVL, N),
	    {ok, lists:append(Acc, AddKVL)}
    end.

-spec read_range_n_on_shards(Name :: string(),
			     Shards :: [string()],
			     StartKey :: key(),
			     N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_shards(Name, Shards, StartKey, N) ->
    %%To be more efficient we can read less number of records from each shard.
    %%NofShards = length(Shards),
    %%Part = (N div NofShards) + 1,
    %%To be safe, currently we try to read N from each shard.
    Args = enterdb:table_info(Name,[data_model,key,columns]),
    KeyDef = proplists:get_value(key, Args),
    {ok, StartKeyDB} = make_db_key(KeyDef, StartKey),

    KVLs =
	[begin
	    {ok, KVL} =
		rpc:call(Node, enterdb_ldb_worker, read_range_n_binary, [Shard, StartKeyDB, N]),
	    KVL
	 end || {Node, Shard} <- Shards],
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls( KVLs ),
    N_KVP = lists:sublist(MergedKVL, N),
    DataModel = proplists:get_value(data_model, Args),
    ColumnsDef = proplists:get_value(columns, Args),
    make_app_kvp(DataModel, KeyDef, ColumnsDef, N_KVP).

-spec order_levels(StartLevel :: string(), Levels :: [string()]) ->
    OrderedLevels :: [string()].
order_levels(StartLevel, [StartLevel|Rest]) ->
     lists:reverse(lists:append(Rest,[StartLevel]));
order_levels(StartLevel, [Level|Rest]) ->
     order_levels(StartLevel, lists:append(Rest,[Level])).

%%--------------------------------------------------------------------
%% @doc
%% Get byte size from each shard of a table and return the sum.
%% @end
%%--------------------------------------------------------------------
-spec approximate_size(Backend :: atom(), Shards :: [#enterdb_shard{}]) ->
    {ok, Size :: pos_integer()} | {error, Reason :: term()}.
approximate_size(leveldb, Shards) ->
    Sizes = [begin
		{ok, Size} = enterdb_ldb_worker:approximate_size(Shard),
		Size
	     end || #enterdb_shard{name = Shard} <- Shards],
    ?debug("Sizes of all shards: ~p", [Sizes]),
    sum_up_sizes(Sizes, 0);
approximate_size(Backend, _) ->
    ?debug("Size approximation is not supported for backend: ~p", [Backend]),
    {error, "backend_not_supported"}.

-spec sum_up_sizes(Sizes :: [pos_integer()], Sum :: pos_integer()) ->
    {ok, Size :: pos_integer()}.
sum_up_sizes([], Sum) ->
    {ok, Sum};
sum_up_sizes([Int | Rest], Sum) when is_integer(Int) ->
    sum_up_sizes(Rest, Sum + Int);
sum_up_sizes([_ | Rest], Sum) ->
    sum_up_sizes(Rest, Sum).
%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration.
%% @end
%%--------------------------------------------------------------------
-spec make_key(TD :: #enterdb_stab{},
		       Key :: [{atom(), term()}]) ->
    {ok, DbKey :: binary} | {error, Reason :: term()}.
make_key(TD, Key) ->
    make_db_key(TD#enterdb_stab.key, Key).

%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and also
%% columns according to DataModel and Columns definition.
%% @end
%%--------------------------------------------------------------------
-spec make_key_columns(TD :: #enterdb_stab{},
		       Key :: [{atom(), term()}],
		       Columns :: term()) ->
    {ok, DbKey :: binary, Columns :: binary} | {error, Reason :: term()}.
make_key_columns(TD, Key, Columns) ->
    case make_db_key(TD#enterdb_stab.key, Key) of
	{error, E} ->
	    {error, E};
	{ok, DBKey} ->
	    make_key_columns_help(DBKey, TD, Columns)
    end.
make_key_columns_help(DBKey, TD, Columns) ->
    case make_db_value(TD#enterdb_stab.data_model,
		       TD#enterdb_stab.columns, Columns) of
	{error, E} ->
	    {error, E};
	{ok, DBValue} ->
	    {ok, DBKey, DBValue}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and
%% provided values in Key.
%% @end
%%--------------------------------------------------------------------
-spec make_db_key(KeyDef :: [atom()],
		  Key :: [{atom(), term()}]) ->
    {ok, DbKey :: binary} | {error, Reason :: term()}.
make_db_key(KeyDef, Key) ->
    KeyDefLen = length(KeyDef),
    KeyLen = length(Key),
    if KeyDefLen == KeyLen ->
	make_db_key(KeyDef, Key, []);
       true ->
        {error, "key_mismatch"}
    end.

-spec make_db_key(KeyDef :: [atom()],
		  Key :: [{atom(), term()}],
		  DBKetList :: [term()]) ->
    ok | {error, Reason::term()}.
make_db_key([], _, DbKeyList) ->
    Tuple = list_to_tuple(lists:reverse(DbKeyList)),
    {ok, term_to_binary(Tuple)};
make_db_key([Field | Rest], Key, DbKeyList) ->
    case lists:keyfind(Field, 1, Key) of
        {_, Value} ->
            make_db_key(Rest, Key, [Value | DbKeyList]);
        false ->
            {error, "key_mismatch"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Make DB value according to DataModel and Columns Definition that is
%% in table configuration and provided values in Columns.
%% @end
%%--------------------------------------------------------------------
-spec make_db_value(DataModel :: data_model(),
		    Columnsdef :: [atom()],
		    Columns :: [{atom(), term()}])->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_value(binary, _, Columns) ->
    {ok, term_to_binary(Columns)};
make_db_value(array, ColumnsDef, Columns) ->
    make_db_array_value(ColumnsDef, Columns);
make_db_value(hash, ColumnsDef, Columns) ->
    make_db_hash_value(ColumnsDef, Columns).

-spec make_db_array_value(ColumnsDef :: [atom()],
			  Columns :: [{atom(), term()}]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_array_value(ColumnsDef, Columns) ->
    ColDefLen = length(ColumnsDef),
    ColLen = length(Columns),
    if ColDefLen == ColLen ->
        make_db_array_value(ColumnsDef, Columns, []);
       true ->
        {error, "column_mismatch"}
    end.

-spec make_db_array_value(ColumnsDef :: [atom()],
		          Columns :: [{atom(), term()}],
		          DbValueList :: [term()]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_array_value([], _Columns, DbValueList) ->
    Tuple = list_to_tuple(lists:reverse(DbValueList)),
    {ok, term_to_binary(Tuple)};
make_db_array_value([Field|Rest], Columns, DbValueList) ->
    case lists:keyfind(Field, 1, Columns) of
        {_, Value} ->
            make_db_array_value(Rest, Columns, [Value|DbValueList]);
        false ->
            {error, "column_mismatch"}
    end.

-spec make_db_hash_value(ColumnsDef :: [atom()],
		         Columns :: [{atom(), term()}]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_hash_value(_ColumnsDef, Columns) ->
    Map = maps:from_list(Columns),
    {ok, term_to_binary(Map)}.

%%--------------------------------------------------------------------
%% @doc
%% Make DB Indexes according to Index Definitons defined in table
%% configuration and provided Cloumns.
%% @end
%%--------------------------------------------------------------------
-spec make_db_indexes(Indexes::[atom()],
		      Columns::[atom()] ) ->
    {ok, DbIndexes::[{atom(),term()}]} | {error, Reason::term()}.
make_db_indexes([],_) ->
    {ok, []};
make_db_indexes(_, _)->
    {error, "not_supported_yet"}.

%%--------------------------------------------------------------------
%% @doc
%% Make app key according to Key Definition defined in table
%% configuration and provided value DBKey.
%% @end
%%--------------------------------------------------------------------
-spec make_app_key(KeyDef :: [atom()],
		   DbKey :: binary()) ->
    AppKey :: key().
make_app_key(KeyDef, DbKey)->
    lists:zip(KeyDef, tuple_to_list(binary_to_term(DbKey))).

%%--------------------------------------------------------------------
%% @doc
%% Make application value according to Columns Definition defined in
%% table configuration and DB Value.
%% Takes internal record #enterdb_stab{} as argument carrying model and
%% columns definitions.
%% @end
%%--------------------------------------------------------------------
-spec make_app_value(TD :: #enterdb_stab{},
		     DBValue :: {ok, binary()} | {error, Reason::term()})->
    Columns :: [term()].
make_app_value(_TD, {error, R}) ->
    {error, R};
make_app_value(TD, {ok, DBValue}) ->
    #enterdb_stab{data_model = DataModel,
		  columns    = ColumnsDef} = TD,
    make_app_value(DataModel, ColumnsDef, DBValue).

%%--------------------------------------------------------------------
%% @doc
%% Make application value according to Columns Definition defined in
%% table configuration and DB Value.
%% @end
%%--------------------------------------------------------------------
-spec make_app_value(DataModel :: data_model(),
		     ColumnsDef :: [atom()],
		     DBValue :: binary()) ->
    Columns :: [term()].

make_app_value(DataModel, ColumnsDef, DBValue) when not is_binary(DBValue)  ->
    format_app_value(DataModel, ColumnsDef, DBValue);
make_app_value(DataModel, ColumnsDef, DBValue) ->
    format_app_value(DataModel, ColumnsDef, binary_to_term(DBValue)).

-spec format_app_value(DataModel :: data_model(),
		       ColumnsDef :: [atom()],
		       Value :: term()) ->
    Columns :: [{atom(), term()}].
format_app_value(binary, _, Columns) ->
    Columns;
format_app_value(array, ColumnsDef, Value) ->
    Columns = tuple_to_list(Value),
    lists:zip(ColumnsDef, Columns);
format_app_value(hash, _, Value) ->
    maps:to_list(Value).

%%--------------------------------------------------------------------
%% @doc
%% Format a key/value list or key/value pair of binaries
%% according to table's data model.
%% @end
%%--------------------------------------------------------------------
-spec make_app_kvp(DataModel :: data_model(),
		   KeyDef :: [atom()],
		   ColumnsDef :: [atom()],
		   KVP :: {binary(), binary()} |
			  [{binary(), binary()}]) ->
    {ok, [{key(), value()}]} | {error, Reason :: term()}.
make_app_kvp(DataModel, KeyDef, ColumnsDef, KVP) ->
    AppKVP =
	case KVP of
	    [_|_] ->
		[begin
		    K = enterdb_lib:make_app_key(KeyDef, BK),
		    V = enterdb_lib:make_app_value(DataModel, ColumnsDef, BV),
		    {K, V}
		 end || {BK, BV} <- KVP];
	    {BinKey, BinValue} ->
		{enterdb_lib:make_app_key(KeyDef, BinKey),
		 enterdb_lib:make_app_value(DataModel, ColumnsDef, BinValue)};
	    [] ->
		[];
	    _ ->
		{error, {invalid_arg, KVP}}
	end,
    {ok, AppKVP}.

-spec comparator_to_dir(Comparator :: ascending | descending) -> 0 | 1.
comparator_to_dir(ascending) ->
    1;
comparator_to_dir(descending) ->
    0.
