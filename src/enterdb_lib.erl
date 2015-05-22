%%%-------------------------------------------------------------------
%%% @author erdem aksu <erdem@sitting>
%%% @copyright (C) 2015, Mobile Arts AB
%%% @doc
%%% Enterdb the key/value storage.
%%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_lib).

%% API
-export([verify_create_table_args/1,
         get_shards/4,
         create_table/1,
         open_leveldb_db/1,
	 close_leveldb_db/1,
	 delete_leveldb_db/1,
	 read_range/3,
	 read_range_n/3,
	 approximate_size/2]).

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
verify_table_options([]) ->
    ok;
verify_table_options([{time_ordered, Bool}|Rest]) when Bool == true;
                                                       Bool == false ->
    verify_table_options(Rest);
verify_table_options([{backend, Backend}|Rest]) when Backend == leveldb;
                                                     Backend == ets_leveldb ->
    verify_table_options(Rest);
verify_table_options([{data_model, DM}|Rest]) when DM == binary;
                                                   DM == array;
                                                   DM == hash ->
    verify_table_options(Rest);
verify_table_options([{wrapped, {FileMargin, TimeMargin}}|Rest]) when is_integer( FileMargin ) andalso
								      FileMargin > 0 andalso
								      is_integer( TimeMargin ) andalso
								      TimeMargin > 0 ->
    verify_table_options(Rest);
verify_table_options([{mem_wrapped, {BucketSpan, NumBuckets}}|Rest]) when is_integer( BucketSpan ) andalso
									  BucketSpan > 0 andalso
									  is_integer( NumBuckets ) andalso
									  NumBuckets > 0 ->
    verify_table_options(Rest);
verify_table_options([Elem|_])->
    {error, {Elem, "invalid_option"}}.

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
%% Create and return list of #enterdb_shard records for local sharding.
%% @end
%%--------------------------------------------------------------------
-spec get_shards(Name :: string(),
		 NumOfShards :: pos_integer(),
		 Wrapped :: undefined | wrapper(),
		 MemWrapped :: undefined | mem_wrapper()) ->
    {ok, [#enterdb_shard{}]}.
get_shards(Name, NumOfShards, undefined, _MemWrapped) ->
    Shards = [lists:concat([Name,"_shard",N]) ||N <- lists:seq(0, NumOfShards-1)],
    Options = [{algorithm, sha}, {strategy, uniform}],
    gb_hash:create_ring(Name, Shards, Options),
    EnterdbShards = [#enterdb_shard{name = ShardName,
				    subdir = Name}
		     || ShardName <- Shards],
    {ok, EnterdbShards};
get_shards(Name, NumOfShards, {FileMargin, TimeMargin}, MemWrapped) ->
    Levels = [lists:concat([Name,"_lev",N]) ||N <- lists:seq(0, FileMargin-1)],
    
    LeveledShards = [ {L,[ lists:concat([L,"_shard",N])
			    || N <- lists:seq(0, NumOfShards-1)] }
			|| L <- Levels ],
    TableType = get_table_type(MemWrapped),
    gb_hash:create_ring(Name, Levels, [{algorithm, {TableType, FileMargin, TimeMargin}},
				       {strategy, timedivision}]),
    Options = [{algorithm, sha}, {strategy, uniform}],
    Shards =
	lists:foldl(fun({NameLev, Ss}, Acc) ->
			gb_hash:create_ring(NameLev, Ss, Options),
			Subdir =  Name++"/"++NameLev,
			EnterdbShards =
			    [#enterdb_shard{name = ShardName,
					    subdir = Subdir}
			     || ShardName <- Ss],
			EnterdbShards ++ Acc
		    end, [], LeveledShards),
    {ok, Shards}.

get_table_type(undefined) ->
    tda;
get_table_type({_,_}) ->
    mem_tda.

%%--------------------------------------------------------------------
%% @doc
%% Create table according to table specification
%% @end
%%--------------------------------------------------------------------
-spec create_table(EnterdbTable::#enterdb_table{}) -> ok | {error, Reason::term()}. 
create_table(#enterdb_table{options = Opts} = EnterdbTable)->
    case proplists:get_value(backend, Opts) of
        leveldb ->
            case create_leveldb_db(EnterdbTable) of
                ok -> write_enterdb_table(EnterdbTable);
                {error, Reason} ->
                    ?debug("Could not create leveldb database, error: ~p~n", [Reason]),
                    {error, Reason}
            end;
        ets_leveldb ->
            %% init mem_wrp table
	    Res = enterdb_mem:init_tab(EnterdbTable),
	    ?debug("enterdb_mem:init_tab returned ~p", [Res]),

	    case create_leveldb_db(EnterdbTable) of
                ok -> write_enterdb_table(EnterdbTable);
                {error, Reason} ->
                    ?debug("Could not create leveldb database, error: ~p~n", [Reason]),
                    {error, Reason}
            end;
        Else ->
	    ?debug("Could not create ~p database, error: not_supported yet.~n", [Else]),
            {error, "no_supported_backend"}
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
%% Create leveldb database that is specified by EnterdbTable.
%% @end
%%--------------------------------------------------------------------
-spec create_leveldb_db(EnterdbTable :: #enterdb_table{}) ->
    ok | {error, Reason :: term()}.
create_leveldb_db(EDBT = #enterdb_table{shards = Shards}) ->
    create_leveldb_db([{comparator, 1},
		       {create_if_missing, true},
                       {error_if_exists, true}], EDBT, Shards).

-spec create_leveldb_db(Options :: [term()],
			EDBT :: #enterdb_table{},
			Shards :: [#enterdb_shard{}]) ->
    ok | {error, Reason :: term()}.
create_leveldb_db(_Options, _EDBT, []) ->
    ok;
create_leveldb_db(Options, EDBT,
		  [#enterdb_shard{name = ShardName,
				  subdir = Subdir} | Rest]) ->
    ChildArgs = [{name, ShardName}, {subdir, Subdir},
                 {options, Options}, {tab_rec, EDBT}],
    case supervisor:start_child(enterdb_ldb_sup, [ChildArgs]) of
        {ok, _Pid} ->
            create_leveldb_db(Options, EDBT, Rest);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Open an existing leveldb database specified by #enterdb_table{}.
%% @end
%%--------------------------------------------------------------------
-spec open_leveldb_db(Table :: #enterdb_table{})-> ok | {error, Reason :: term()}.
open_leveldb_db(EDBT = #enterdb_table{shards = Shards}) ->
    Options = [{comparator, 1},
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
%% Limit items.
%% @end
%%--------------------------------------------------------------------
-spec read_range(Name :: string(),
		 Range :: key_range(),
		 Limit :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason::term()}.
read_range(Name, Range, Limit) ->
    case gb_hash:get_nodes(Name) of
	undefined ->
	    {error, "no_table"};
	{ok, {level, Levels}} ->
	    {Start, End} = Range,
	    {ok, {level, StartLevel}} = gb_hash:find_node(Name, Start),
	    {ok, {level, EndLevel}} = gb_hash:find_node(Name, End),
	    LevelRange = get_level_range(StartLevel, EndLevel, Levels),
	    LevelsOfShards = [begin {ok, Ss} = gb_hash:get_nodes(L), Ss end
				|| L <- LevelRange],
	    RangeResults = [read_range_on_shards(hd(LevelRange), LSs, Range, Limit) || LSs <- LevelsOfShards],
	    lists:append([L || {ok, L} <- RangeResults]);
	{ok, Shards} ->
	    read_range_on_shards(Name, Shards, Range, Limit)
    end.


-spec read_range_on_shards(Name :: string(),
			   Shards :: [string()],
			   Range :: key_range(),
			   Limit :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_on_shards(Name, Shards, Range, Limit)->
    KVLs =
	[begin
	    {ok, KVL} = enterdb_ldb_worker:read_range_binary(Shard, Range, Limit),
	    KVL
	 end || Shard <- Shards],
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls( KVLs ),
    {ok, AnyShard} = gb_hash:find_node(Name, Range),
    enterdb_ldb_worker:make_app_kvp(AnyShard, MergedKVL).

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
%% @doc
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
	{ok, {level, Levels}} ->
	    {ok, {level, StartLevel}} = gb_hash:find_node(Name, StartKey),
	    OrderedLevels = order_levels(StartLevel, Levels),
	    read_range_n_on_levels(OrderedLevels, StartKey, N, []);
	{ok, Shards} ->
	    read_range_n_on_shards(Shards, StartKey, N)
    end.

-spec read_range_n_on_levels(Levels :: [string()],
			     StartKey :: key(),
			     N :: pos_integer(),
			     Acc :: [kvp()]) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_levels([], _StartKey, _N, Acc) ->
    {ok, Acc};
read_range_n_on_levels([Level|Rest], StartKey, N, Acc) ->
    {ok, Shards} = gb_hash:get_nodes(Level),
    {ok, KVL} = read_range_n_on_shards(Shards, StartKey, N),
    Len = length(KVL),
    if Len =< N ->
	    read_range_n_on_levels(Rest, StartKey, N-Len,
				   lists:append(Acc, KVL));
	true ->
	    AddKVL = lists:sublist(KVL, N),
	    {ok, lists:append(Acc, AddKVL)}
    end.

-spec read_range_n_on_shards(Shards :: [string()],
			     StartKey :: key(),
			     N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_shards(Shards, StartKey, N) ->
    %%To be more efficient we can read less number of records from each shard.
    %%NofShards = length(Shards),
    %%Part = (N div NofShards) + 1,
    %%To be safe, currently we try to read N from each shard.
    KVLs =
	[begin
	    {ok, KVL} = enterdb_ldb_worker:read_range_n_binary(Shard, StartKey, N),
	    KVL
	 end || Shard <- Shards],
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls( KVLs ),
    N_KVP = lists:sublist(MergedKVL, N),
    enterdb_ldb_worker:make_app_kvp(hd(Shards), N_KVP).

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
