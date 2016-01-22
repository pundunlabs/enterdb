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
%% Enterdb the key/value storage library functions.
%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%===================================================================

-module(enterdb_lib).

%% API
-export([verify_create_table_args/1,
         get_shards/3,
         create_table/1,
         open_db/1,
         open_shards/1,
	 close_db/1,
	 read_range_on_shards/4,
	 read_range_n_on_shards/4,
	 approximate_size/2]).

-export([make_db_key/2,
	 make_key/2,
	 make_key_columns/3,
	 make_db_value/3,
	 make_db_indexes/2,
	 make_app_key/2,
	 make_app_value/2,
	 make_app_value/3,
	 make_app_kvp/4,
	 check_error_response/1]).

-export([do_create_shard/2,
	 open_shard/1,
	 close_shard/1,
	 write_enterdb_table/1,
	 get_shard_def/1,
	 update_bucket_list/2,
	 get_tab_def/1,
	 get_table_options/1,
	 delete_shards/1,
	 delete_shard/1,
	 cleanup_table/2,
	 cleanup_table_help/1,
	 reduce_cont/2,
	 cut_kvl_at/2,
	 comparator_to_dir/1]).

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
-spec verify_create_table_args(Args :: [{atom(), term()}]) ->
    {ok, #enterdb_table{}} |
    {error, Reason::term()}.
verify_create_table_args(Args)->
    verify_create_table_args(Args, #enterdb_table{}).

verify_create_table_args([], #enterdb_table{} = EnterdbTable)->
    {ok, EnterdbTable};
verify_create_table_args([{name, Name} | Rest],
			 #enterdb_table{} = EdbTab) when is_list(Name)->
    case verify_name(Name) of
        ok ->
            verify_create_table_args(Rest, EdbTab#enterdb_table{name = Name});
        {error, Reason} ->
            {error, Reason}
    end;
verify_create_table_args([{key, Key}|Rest],
			 #enterdb_table{} = EdbTab) ->
    case verify_key(Key) of
        ok ->
           verify_create_table_args(Rest, EdbTab#enterdb_table{key = Key});
        {error, Reason} ->
           {error, {Key, Reason}}
    end;
verify_create_table_args([{columns, Columns}|Rest],
                          #enterdb_table{key = Key} = EdbTab) ->
    case verify_columns(Columns) of
        ok ->
           OnlyDataColumns = lists:subtract(Columns, Key),
           verify_create_table_args(Rest,
	    EdbTab#enterdb_table{columns = OnlyDataColumns});
        {error, Reason} ->
           {error, {Columns, Reason}}
    end;
verify_create_table_args([{indexes, Indexes}|Rest],
                         #enterdb_table{key = Key,
                                        columns = Columns} = EnterdbTable)
    when is_list(Indexes)->
    case verify_fields(Indexes++Key) of
        ok ->
            {ok, NewColumns} = add_index_fields_to_columns(Indexes, Columns),
            verify_create_table_args(Rest,
		EnterdbTable#enterdb_table{columns = NewColumns,
                                           indexes = Indexes});
        {error, Reason} ->
           {error, {Indexes, Reason}}
    end;
verify_create_table_args([{options, Options}|Rest],
                         #enterdb_table{} = EnterdbTable)
    when is_list(Options)->
    case verify_table_options(Options) of
        ok ->
            verify_create_table_args(Rest,
		EnterdbTable#enterdb_table{options = Options});
        {error, Reason} ->
            {error, Reason}
    end;
verify_create_table_args([{Arg, _}|_], _)->
    {error, {Arg, "not_list"}}.

%%-------------------------------------------------------------------
%% @doc
%% Verify if the given list elements are all strings and the list
%% has unique elements
%% @end
%%-------------------------------------------------------------------
-spec verify_fields(List::[term()]) ->
    ok | {error, Reason::term()}.
verify_fields([])->
    ok;
verify_fields([Elem | Rest]) when is_list(Elem) ->
    case io_lib:printable_list(Elem) of
	true ->
	    case lists:member(Elem, Rest) of
		true ->
		    {error, "dublicate_key"};
		false ->
		    verify_fields(Rest)
	    end;
	false ->
	    {error, "not_printable"}
    end;
verify_fields(_) ->
    {error, "not_list"}.

-spec verify_name(String::string())->
    ok | {error, Reason::term()}.
verify_name(Name) ->
    case verify_name(Name, 0) of
        ok -> check_if_table_exists(Name);
        Error -> Error
    end.

-spec verify_name(String::string(), Acc::non_neg_integer())->
    ok | {error, Reason::term()}.
verify_name(_, Acc) when Acc > ?MAX_TABLE_NAME_LENGTH ->
    {error, "too_long_name"};
verify_name([Char|_Rest], _Acc) when Char > 255 ->
    {error, "non_unicode_name"};
verify_name([_Char|Rest], Acc) ->
    verify_name(Rest, Acc+1);
verify_name([], _) ->
    ok.

-spec verify_key(Key :: [string()]) ->
    ok | {error, Reason :: term()}.
verify_key(Key) when is_list(Key) ->
    case length(Key) of
	Len when Len < 1 ->
	    {error, "no_key_field"};
	Len when Len > 100 ->
	    {error, "key_too_long"};
	_ ->
	    verify_fields(Key)
    end;
verify_key(_) ->
    {error, "invalid_key"}.

-spec verify_columns(Columns :: [string()]) ->
    ok | {error, Reason :: term()}.
verify_columns(Columns) when is_list(Columns) ->
    case length(Columns) of
	Len when Len < 1 ->
	    {error, "no_columns_field"};
	Len when Len > 10000 ->
	    {error, "too_many_columns"};
	_ ->
	    verify_fields(Columns)
    end;
verify_columns(_) ->
    {error, "invalid_columns"}.

%%-------------------------------------------------------------------
%% @doc
%% Get table definition
%% @end
%%-------------------------------------------------------------------
-spec get_tab_def(string()) ->
    #enterdb_table{} | {error, Reason::term()}.
get_tab_def(Tab) ->
    case mnesia:dirty_read(enterdb_table, Tab) of
	[TabDef] ->
	    TabDef;
	_ ->
	    {error, "no_table"}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Get shard definition
%% @end
%%-------------------------------------------------------------------
-spec get_shard_def(string()) ->
    #enterdb_stab{} | {error, Reason::term()}.
get_shard_def(Shard) ->
    case mnesia:dirty_read(enterdb_stab, Shard) of
	[ShardTab] ->
	    ShardTab;
	_ ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Update the #enterdb_stab entry in mnesia disc_copy with new bucket
%% list for a given shard
%% @end
%%--------------------------------------------------------------------
-spec update_bucket_list(ShardName :: shard_name(),
			 Buckets :: [shard_name()]) ->
    ok | {error, Reason :: term()}.
update_bucket_list(ShardName, Buckets) ->
    Fun =
	fun() ->
	    [EnterdbShard] = mnesia:read(enterdb_stab, ShardName),
	    mnesia:write(EnterdbShard#enterdb_stab{buckets=Buckets})
	end,
    case enterdb_db:transaction(Fun) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

-spec add_index_fields_to_columns(Indexes::[string()], Columns::[string()]) ->
    {ok, NewColumns::[string()]}.
add_index_fields_to_columns([], Columns)->
    {ok, Columns};
add_index_fields_to_columns([Elem|Rest], Columns)->
    case lists:member(Elem, Columns) of
        true ->
            add_index_fields_to_columns(Rest, Columns);
        fasle ->
            add_index_fields_to_columns(Rest, Columns++[Elem])
    end.

-spec verify_table_options(Options::[table_option()]) ->
    ok | {error, Reason::term()}.
%% Pre configured clusters
verify_table_options([{clusters, Clusters}|Rest])
when is_list(Clusters) ->
    verify_table_options(Rest);

%% Number of Shards
verify_table_options([{shards, NumOfShards}|Rest])
when is_integer(NumOfShards), NumOfShards > 0 ->
    verify_table_options(Rest);

%% Table types
verify_table_options([{type, Type}|Rest])
    when
	 Type =:= leveldb;
         %Type =:= ets_leveldb;
	 Type =:= leveldb_wrapped
	 %Type =:= ets_levedb_wrapped
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
verify_table_options([{wrapper, Wrapper} | Rest]) ->
    case verify_wrapper(Wrapper) of
        ok ->
	    verify_table_options(Rest);
        {error, Reason} ->
	    {error, Reason}
    end;

%% wrapping details for ets part of wrapped db
verify_table_options([{mem_wrapper, {BucketSpan, NumBuckets}}|Rest])
    when
	is_integer( BucketSpan ), BucketSpan > 0,
	is_integer( NumBuckets ), NumBuckets > 0
    ->
	verify_table_options(Rest);
%% comparator defines how the keys will be sorted
verify_table_options([{comparator, C}|Rest]) when C == descending;
						  C == ascending ->
    verify_table_options(Rest);
%% time_series states the key is compound and contains a timestamp
%% These keys will be hashed without but sorted with timestamp value
verify_table_options([{time_series, T}|Rest]) when is_boolean(T) ->
    verify_table_options(Rest);
%% Bad Option
verify_table_options([Elem|_])->
    {error, {Elem, "invalid_option"}};
%% All Options OK
verify_table_options([]) ->
    ok.

-spec verify_wrapper(Wrapper :: #enterdb_wrapper{}) ->
    ok | {error, Reason :: term()}.
verify_wrapper(#enterdb_wrapper{time_margin = undefined,
				size_margin = undefined} = Wrp) ->
    {error, {Wrp, "invalid_option"}};
verify_wrapper(#enterdb_wrapper{num_of_buckets = NumOfBuckets,
				time_margin = TimeMargin,
				size_margin = SizeMargin} = Wrp)
    when is_integer(NumOfBuckets), NumOfBuckets > 2 ->
    TM = valid_time_margin(TimeMargin),
    SM = valid_size_margin(SizeMargin),
    case (TM or SM) of
	true -> ok;
	false -> {error, {Wrp, "invalid_option"}}
    end;
verify_wrapper(Elem)->
    {error, {Elem, "invalid_option"}}.

-spec valid_time_margin(TimeMargin :: time_margin()) ->
    true | false.
valid_time_margin({seconds, Time}) when is_integer(Time), Time > 0 ->
    true;
valid_time_margin({minutes, Time}) when is_integer(Time), Time > 0 ->
    true;
valid_time_margin({hours, Time}) when is_integer(Time), Time > 0 ->
    true;
valid_time_margin(_) ->
    false.

-spec valid_size_margin(SizeMargin :: size_margin()) ->
    true | false.
valid_size_margin({megabytes, Size}) when is_integer(Size), Size > 0 ->
    true;
valid_size_margin(_) ->
    false.

-spec check_if_table_exists(Name :: string()) ->
    ok | {error, Reason :: term()}.
check_if_table_exists(Name)->
    case enterdb_db:transaction(fun() ->
				    mnesia:read(enterdb_table, Name)
				end) of
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
    TD = get_shard_def(Shard),
    case mnesia:dirty_read(enterdb_table, TD#enterdb_stab.name) of
	[#enterdb_table{options = Options}] ->
	    {ok, Options};
	_ ->
	    {error, no_table}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check response for error
%% @end
%%--------------------------------------------------------------------
-spec check_error_response(RespList :: list) ->
    ok | {error, RespList :: list}.
check_error_response([ok]) ->
    ok;
check_error_response(ResponseList) ->
    {error, ResponseList}.

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
%% Create and return list of {Node, Shard} tuples for all shards
%% @end
%%--------------------------------------------------------------------
-spec get_shards(Name :: string(),
		 NumOfShards :: pos_integer(),
		 Nodes :: [node()]) ->
    {ok, [{node(), string()}]}.
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
-spec create_table(EnterdbTable::#enterdb_table{}) ->
    ok | {error, Reason::term()}.
create_table(#enterdb_table{shards = Shards} = EnterdbTable)->
    ShardRes =
	[rpc:call(Node, ?MODULE, do_create_shard, [Shard, EnterdbTable])
	    || {Node, Shard} <- Shards],
    case lists:usort(ShardRes) of
	[ok] -> %% all shards was created, lets write enterdb_table
	    SchemaRes =
		[ rpc:call(Node, ?MODULE, write_enterdb_table, [EnterdbTable])
		    || Node <- lists:usort([Node || {Node, _} <- Shards])],
		%% TODO: do error handling (rollback and such)
		check_error_response(lists:usort(SchemaRes));

	R ->
	   check_error_response(R)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creating shard on local node.
%% @end
%%--------------------------------------------------------------------
-spec do_create_shard(Shard :: shard_name(),
		      EDBT :: #enterdb_table{}) ->
    ok | {error, Reason :: term()}.
do_create_shard(Shard, EDBT) ->
    Options = EDBT#enterdb_table.options,
    DataModel = EDBT#enterdb_table.data_model,
    Wrapper = proplists:get_value(wrapper, Options),
    Buckets = get_buckets(Shard, EDBT#enterdb_table.type, Wrapper),
    ESTAB = #enterdb_stab{shard = Shard,
			  name = EDBT#enterdb_table.name,
			  type = EDBT#enterdb_table.type,
			  key  = EDBT#enterdb_table.key,
			  columns = EDBT#enterdb_table.columns,
			  indexes = EDBT#enterdb_table.indexes,
			  comparator = EDBT#enterdb_table.comparator,
			  data_model = DataModel,
			  wrapper = Wrapper,
			  buckets = Buckets},
    write_shard_table(ESTAB),
    do_create_shard_type(ESTAB).

-spec do_create_shard_type(ESTAB :: #enterdb_stab{}) ->
    ok.
do_create_shard_type(#enterdb_stab{type = leveldb} = ESTAB) ->
    create_leveldb_shard(ESTAB);

do_create_shard_type(#enterdb_stab{type = leveldb_wrapped} = ESTAB) ->
    create_leveldb_wrp_shard(ESTAB);

do_create_shard_type(#enterdb_stab{type = ets_leveldb} = ESTAB) ->
    %% TODO: init LRU-Cache here as well
    create_leveldb_shard(ESTAB);

do_create_shard_type(#enterdb_stab{type = ets_leveldb_wrapped} = ESTAB)->
    %% TODO: init wrapping LRU-Cache here as well
    create_leveldb_shard(ESTAB).

%%--------------------------------------------------------------------
%% @doc
%% Open an existing enterdb database shard.
%% @end
%%--------------------------------------------------------------------
-spec open_shard(Name :: string())->
    ok | {error, Reason :: term()}.
open_shard(Name) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_stab, Name) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [ShardTab]} ->
	     do_open_shard(ShardTab);
	{error, Reason} ->
            {error, Reason}
    end.

%% Open existing shard locally
do_open_shard(#enterdb_stab{type = leveldb} = EDBT) ->
    open_leveldb_shard(EDBT);
do_open_shard(#enterdb_stab{type = leveldb_wrapped} = EDBT) ->
    open_leveldb_wrp_shard(EDBT);
do_open_shard(#enterdb_stab{type = ets_leveldb} = EDBT) ->
    %% TODO: init LRU-Cache here as well
    open_leveldb_shard(EDBT);
do_open_shard(Else)->
    ?debug("enterdb:close_table: {type, ~p} not supported", [Else]),
    {error, "type_not_supported"}.

-spec close_shard(Shard :: shard_name()) ->
    ok | {error, Reason :: term()}.
close_shard(Shard) ->
    case enterdb_db:transaction(fun() -> mnesia:read(enterdb_stab, Shard) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [ShardTab]} ->
	     do_close_shard(ShardTab);
	{error, Reason} ->
            {error, Reason}
    end.

-spec do_close_shard(ESTAB :: #enterdb_stab{}) ->
    ok.
do_close_shard(#enterdb_stab{shard=Shard,
			     type = leveldb})->
    supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(Shard));
do_close_shard(#enterdb_stab{shard=Shard,
			     type = leveldb_wrapped})->
    enterdb_ldb_wrp:close_shard(Shard);
do_close_shard(Else)->
    ?debug("enterdb:close_table: {type, ~p} not supported", [Else]),
    {error, "type_not_supported"}.

%% create leveldb shard
%% TODO: move out to levedb specific lib.
-spec create_leveldb_shard(ESTAB :: #enterdb_stab{}) ->
    ok.
create_leveldb_shard(ESTAB) ->
    Options = [{comparator, ESTAB#enterdb_stab.comparator},
	       {create_if_missing, true},
	       {error_if_exists, true}],
    ChildArgs = [{name, ESTAB#enterdb_stab.shard},
		 {subdir, ESTAB#enterdb_stab.name},
                 {options, Options}, {tab_rec, ESTAB}],
    {ok, _Pid} = supervisor:start_child(enterdb_ldb_sup, [ChildArgs]),
    ok.

%% open leveldb shard
%% TODO: move out to levedb specific lib.
-spec open_leveldb_shard(ESTAB :: #enterdb_stab{}) ->
    ok.
open_leveldb_shard(ESTAB) ->
    Options = [{comparator, ESTAB#enterdb_stab.comparator},
	       {create_if_missing, false},
	       {error_if_exists, false}],
    ChildArgs = [{name, ESTAB#enterdb_stab.shard},
		 {subdir, ESTAB#enterdb_stab.name},
                 {options, Options}, {tab_rec, ESTAB}],
    {ok, _Pid} = supervisor:start_child(enterdb_ldb_sup, [ChildArgs]),
    ok.

-spec create_leveldb_wrp_shard(ESTAB :: #enterdb_stab{}) ->
    ok.
create_leveldb_wrp_shard(#enterdb_stab{wrapper = undefined} = ESTAB) ->
    create_leveldb_shard(ESTAB);
create_leveldb_wrp_shard(#enterdb_stab{shard = Shard,
				       wrapper = Wrapper,
				       buckets = Buckets} = ESTAB) ->

    Options = [{comparator, ESTAB#enterdb_stab.comparator},
	       {create_if_missing, true},
	       {error_if_exists, true}],
    ChildArgs = [{subdir, ESTAB#enterdb_stab.name},
                 {options, Options}, {tab_rec, ESTAB}],

    ok = enterdb_ldb_wrp:init_buckets(Shard, Buckets, Wrapper),
    [{ok, _Pid} = supervisor:start_child(enterdb_ldb_sup,
					 [[{name, Bucket} | ChildArgs]]) ||
	Bucket <- Buckets],
    ok.

-spec open_leveldb_wrp_shard(ESTAB :: #enterdb_stab{}) ->
    ok.
open_leveldb_wrp_shard(#enterdb_stab{wrapper = undefined} = ESTAB) ->
    open_leveldb_shard(ESTAB);
open_leveldb_wrp_shard(#enterdb_stab{shard = Shard,
				     wrapper = Wrapper,
				     buckets = Buckets} = ESTAB) ->
    Options = [{comparator, ESTAB#enterdb_stab.comparator},
	       {create_if_missing, false},
	       {error_if_exists, false}],
    ChildArgs = [{subdir, ESTAB#enterdb_stab.name},
                 {options, Options}, {tab_rec, ESTAB}],

    ok = enterdb_ldb_wrp:init_buckets(Shard, Buckets, Wrapper),
    [{ok, _Pid} = supervisor:start_child(enterdb_ldb_sup,
					 [[{name, Bucket} | ChildArgs]]) ||
	Bucket <- Buckets],
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Creates bucket names for a wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec get_buckets(Shard :: shard_name(),
		  Type :: type(),
		  Wrapper :: #enterdb_wrapper{}) ->
    [shard_name()] | undefined.
get_buckets(Shard, leveldb_wrapped, Wrapper) ->
    enterdb_ldb_wrp:create_bucket_list(Shard, Wrapper);
get_buckets(_Shard, _, _Wrapper) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_stab entry in mnesia disc_copy
%% @end
%%--------------------------------------------------------------------
-spec write_shard_table(EnterdbShard::#enterdb_stab{}) ->
    ok | {error, Reason :: term()}.
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
-spec write_enterdb_table(EnterdbTable::#enterdb_table{}) ->
    ok | {error, Reason :: term()}.
write_enterdb_table(EnterdbTable) ->
    case enterdb_db:transaction(fun() -> mnesia:write(EnterdbTable) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Open an existing database specified by #enterdb_table{}.
%% @end
%%--------------------------------------------------------------------
-spec open_db(Table :: #enterdb_table{})->
    ok | {error, Reason :: term()}.
open_db(#enterdb_table{shards = Shards}) ->
    Res = [rpc:call(Node, ?MODULE, open_shard, [Name])
	    || {Node, Name} <- Shards],
    check_error_response(lists:usort(Res)).

%%-------------------------------------------------------------------d
%% @doc
%% Open database table shards on defined node.
%% @end
%%--------------------------------------------------------------------
-spec open_shards(ShardList :: [string()]) ->
    ok | {error, Reason :: term()}.
open_shards([]) ->
    ok;
open_shards([Shard | Rest]) ->
    ?debug("Opening Shard: ~p",[Shard]),
    case open_shard(Shard) of
	ok ->
	    open_shards(Rest);
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Close an existing leveldb database specified by #enterdb_table{}.
%% @end
%%--------------------------------------------------------------------
-spec close_db(Table :: #enterdb_table{}) ->
    ok | {error, Reason :: term()}.
close_db(#enterdb_table{shards = Shards}) ->
    Res = [rpc:call(Node, ?MODULE, close_shard, [Name])
	    || {Node, Name} <- Shards],
    check_error_response(lists:usort(Res)).

%%--------------------------------------------------------------------
%% @doc
%% Delete an existing table shards.
%% This function should be called within a mnesia transaction.
%% @end
%%--------------------------------------------------------------------
-spec delete_shards([{Node :: atom(), Shard :: string()}]) ->
    ok | {error, Reason :: term()}.
delete_shards([{Node, Shard} | Rest]) ->
    rpc:call(Node, ?MODULE, delete_shard, [Shard]),
    delete_shards(Rest);
delete_shards([]) ->
    ok.

delete_shard(Shard) ->
    SD = get_shard_def(Shard),
    ok = delete_shard_help(SD),
    mnesia:dirty_delete(enterdb_stab, Shard).

%% add delete per type
delete_shard_help(#enterdb_stab{shard = Name, type = leveldb}) ->
    enterdb_ldb_worker:delete_db(Name),
    ok;
delete_shard_help(#enterdb_stab{shard = Name, type = leveldb_wrapped}) ->
    enterdb_ldb_wrp:delete_shard(Name),
    ok;
delete_shard_help({error, Reason}) ->
    {error, Reason}.

cleanup_table(Name, Shards) ->
    Nodes = lists:usort([Node || {Node, _} <- Shards]),
    AllRes = [rpc:call(Node, ?MODULE, cleanup_table_help, [Name]) ||
		Node <- Nodes],
    case lists:usort(AllRes) of
	[Res] ->
	    Res;
	E ->
	    E
    end.

cleanup_table_help(Name) ->
    mnesia:dirty_delete(enterdb_table, Name),
    gb_hash:delete_ring(Name).

%%--------------------------------------------------------------------
%% @doc
%% Reads a Range of Keys from table Tab from Nodes and returns max
%% Chunk items.
%% @end
%%--------------------------------------------------------------------
-spec read_range_on_shards({ok, Nodes :: [{atom(), string()}]} | undefined,
			   Tab :: #enterdb_table{},
			   {StartKey :: binary(), StopKey :: binary()},
			   Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} | {error, Reason :: term()}.
read_range_on_shards({ok, Nodes},
		     Tab = #enterdb_table{key = KeyDef,
					  type = Type,
					  comparator = Comp},
		     RangeDB, Chunk)->
    Dir = comparator_to_dir(Comp),
    {CallbackMod, TrailingArgs} =
	case Type of
	    leveldb -> {enterdb_ldb_worker, []};
	    ets_leveldb -> {enterdb_ldb_worker, []};
	    leveldb_wrapped -> {enterdb_ldb_wrp, [Dir]};
	    ets_leveldb_wrapped -> {enterdb_ldb_worker, []}
	end,

    KVLs_and_Conts =
	[begin
	    Args = [Shard, RangeDB, Chunk | TrailingArgs],
	    {ok, KVL, Cont} =
		rpc:call(Node, CallbackMod, read_range_binary, Args),
	    {KVL, Cont}
	 end || {Node, Shard} <- Nodes],
    {KVLs, Conts} = lists:unzip(KVLs_and_Conts),

    ContKeys = [K || K <- Conts, K =/= complete],
    {ok, KVL, ContKey} = merge_and_cut_kvls(Dir, KeyDef, KVLs, ContKeys),

    {ok, ResultKVL} = make_app_kvp(Tab, KVL),
    {ok, ResultKVL, ContKey}.

-spec merge_and_cut_kvls(Dir :: 0 | 1,
			 KeyDef :: [string()],
			 KVLs :: [[kvp()]],
			 ContKeys :: [binary()]) ->
    {ok, KVL :: [kvp()]}.
merge_and_cut_kvls(Dir, _KeyDef, KVLs, []) ->
   {ok, KVL} = leveldb_utils:merge_sorted_kvls(Dir, KVLs),
   {ok, KVL, complete};
merge_and_cut_kvls(Dir, KeyDef, KVLs, ContKeys) ->
    {Cont, _} = ContKVP = reduce_cont(Dir, ContKeys),
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls(Dir, [[ContKVP]|KVLs]),
    ContKey =  make_app_key(KeyDef, Cont),
    {ok, cut_kvl_at(Cont, MergedKVL), ContKey}.

-spec reduce_cont(Comparator :: comparator(),
		  Conts :: [binary()]) ->
    {key(), binary()}.
reduce_cont(Dir, ContKeys) ->
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

%%--------------------------------------------------------------------
%% @doc
%% Reads a N number of Keys starting from DBStartKey from each shard
%% that is given by Nodes and merges collected key/value lists.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n_on_shards({ok, Nodes :: [{atom(), string()}]} | undefined,
			     Tab :: #enterdb_table{},
			     DBStartKey :: binary(),
			     N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_shards(undefined, _Tab, _DBStartKey, _N) ->
     {error, "no_table"};
read_range_n_on_shards({ok, Nodes},
		       Tab = #enterdb_table{type = Type,
					    comparator = Comp},
		       DBStartKey, N) ->
    ?debug("DBStartKey: ~p, Nodes: ~p",[DBStartKey, Nodes]),
    Dir = comparator_to_dir(Comp),
    {CallbackMod, TrailingArgs} =
	case Type of
	    leveldb -> {enterdb_ldb_worker, []};
	    ets_leveldb -> {enterdb_ldb_worker, []};
	    leveldb_wrapped -> {enterdb_ldb_wrp, [Dir]};
	    ets_leveldb_wrapped -> {enterdb_ldb_worker, []}
	end,
    %%To be more efficient we can read less number of records from each shard.
    %%NofShards = length(Shards),
    %%Part = (N div NofShards) + 1,
    %%To be safe, currently we try to read N from each shard.
    KVLs =
	[begin
	    Args = [Shard, DBStartKey, N | TrailingArgs],
	    {ok, KVL} =
		rpc:call(Node, CallbackMod, read_range_n_binary, Args),
	    ?debug("KVL on ~p: ~p",[Shard, KVL]),
	    KVL
	 end || {Node, Shard} <- Nodes],
    ?debug("KVLs: ~p",[KVLs]),
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls(Dir, KVLs),
    N_KVP = lists:sublist(MergedKVL, N),
    make_app_kvp(Tab, N_KVP).

%%--------------------------------------------------------------------
%% @doc
%% Get byte size from each shard of a table and return the sum.
%% @end
%%--------------------------------------------------------------------
-spec approximate_size(Backend :: string(),
		       Shards :: [{Node :: atom(), Shard :: string()}]) ->
    {ok, Size :: pos_integer()} | {error, Reason :: term()}.
approximate_size(leveldb, Shards) ->
    Sizes =
	[begin
	    {ok, Size} =
		rpc:call(Node, enterdb_ldb_worker, approximate_size, [Shard]),
		Size
	 end || {Node, Shard} <- Shards],
    ?debug("Sizes of all shards: ~p", [Sizes]),
    sum_up_sizes(Sizes, 0);
approximate_size(Type, _) ->
    ?debug("Size approximation is not supported for type: ~p", [Type]),
    {error, "type_not_supported"}.

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
-spec make_key(TD :: #enterdb_table{},
		       Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary} | {error, Reason :: term()}.
make_key(TD, Key) ->
    make_db_key(TD#enterdb_table.key, Key).

%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and also
%% columns according to DataModel and Columns definition.
%% @end
%%--------------------------------------------------------------------
-spec make_key_columns(TableDef :: #enterdb_table{},
		       Key :: [{string(), term()}],
		       Columns :: term()) ->
    {ok, DbKey :: binary, Columns :: binary} | {error, Reason :: term()}.
make_key_columns(TD, Key, Columns) ->
    case make_db_key(TD#enterdb_table.key, Key) of
	{error, E} ->
	    {error, E};
	{ok, DBKey} ->
	    make_key_columns_help(DBKey, TD, Columns)
    end.
make_key_columns_help(DBKey, TD, Columns) ->
    case make_db_value(TD#enterdb_table.data_model,
		       TD#enterdb_table.columns, Columns) of
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
-spec make_db_key(KeyDef :: [string()],
		  Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary} | {error, Reason :: term()}.
make_db_key(KeyDef, Key) ->
    KeyDefLen = length(KeyDef),
    KeyLen = length(Key),
    if KeyDefLen == KeyLen ->
	make_db_key(KeyDef, Key, []);
       true ->
        {error, "key_mismatch"}
    end.

-spec make_db_key(KeyDef :: [string()],
		  Key :: [{string(), term()}],
		  DBKeyList :: [term()]) ->
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
		    Columnsdef :: [string()],
		    Columns :: [{string(), term()}])->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_value(binary, _, Columns) ->
    {ok, term_to_binary(Columns)};
make_db_value(array, ColumnsDef, Columns) ->
    make_db_array_value(ColumnsDef, Columns);
make_db_value(hash, ColumnsDef, Columns) ->
    make_db_hash_value(ColumnsDef, Columns).

-spec make_db_array_value(ColumnsDef :: [string()],
			  Columns :: [{string(), term()}]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_array_value(ColumnsDef, Columns) ->
    ColDefLen = length(ColumnsDef),
    ColLen = length(Columns),
    if ColDefLen == ColLen ->
        make_db_array_value(ColumnsDef, Columns, []);
       true ->
        {error, "column_mismatch"}
    end.

-spec make_db_array_value(ColumnsDef :: [string()],
		          Columns :: [{string(), term()}],
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

-spec make_db_hash_value(ColumnsDef :: [string()],
		         Columns :: [{string(), term()}]) ->
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
-spec make_db_indexes(Indexes::[string()],
		      Columns::[string()] ) ->
    {ok, DbIndexes::[{string(), term()}]} | {error, Reason::term()}.
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
-spec make_app_key(KeyDef :: [string()],
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
    {ok, make_app_value(DataModel, ColumnsDef, DBValue)}.

%%--------------------------------------------------------------------
%% @doc
%% Make application value according to Columns Definition defined in
%% table configuration and DB Value.
%% @end
%%--------------------------------------------------------------------
-spec make_app_value(DataModel :: data_model(),
		     ColumnsDef :: [string()],
		     DBValue :: binary()) ->
    Columns :: [term()].
make_app_value(DataModel, ColumnsDef, DBValue) when not is_binary(DBValue)  ->
    format_app_value(DataModel, ColumnsDef, DBValue);
make_app_value(DataModel, ColumnsDef, DBValue) ->
    format_app_value(DataModel, ColumnsDef, binary_to_term(DBValue)).

-spec format_app_value(DataModel :: data_model(),
		       ColumnsDef :: [string()],
		       Value :: term()) ->
    Columns :: [{string(), term()}].
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
-spec make_app_kvp(Tab :: #enterdb_table{},
		   KVP :: {binary(), binary()} |
			  [{binary(), binary()}]) ->
    {ok, [{key(), value()}]} | {error, Reason :: term()}.
make_app_kvp(#enterdb_table{key = KeyDef,
			    columns = ColumnsDef,
			    data_model = DataModel}, KVP) ->
    make_app_kvp(DataModel, KeyDef, ColumnsDef, KVP).

%%--------------------------------------------------------------------
%% @doc
%% Format a key/value list or key/value pair of binaries
%% according to table's data model.
%% @end
%%--------------------------------------------------------------------
-spec make_app_kvp(DataModel :: data_model(),
		   KeyDef :: [string()],
		   ColumnsDef :: [string()],
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

-spec comparator_to_dir(Comparator :: descending | ascending) ->
    0 | 1.
comparator_to_dir(descending) ->
    0;
comparator_to_dir(ascending) ->
    1.
