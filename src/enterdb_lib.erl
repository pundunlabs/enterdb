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
-export([get_path/1,
	 get_num_of_local_shards/0]).

-export([verify_create_table_args/1,
	 create_table/1,
         open_table/2,
         open_shards/1,
	 close_table/2,
	 read_range_on_shards/4,
	 read_range_n_on_shards/4,
	 approximate_size/3]).

-export([make_db_key/2,
	 make_db_key/3,
	 make_key/2,
	 make_key_cids/2,
	 make_key_columns/3,
	 make_db_value/4,
	 make_app_key/2,
	 make_app_value/2,
	 make_app_value/3,
	 make_app_kvp/4,
	 apply_update_op/3,
	 encode_unsigned/2,
	 get_hash_key_def/3,
	 check_error_response/1,
	 map_shards_seq/3,
	 map_shards/4]).


-export([open_shard/1,
	 close_shard/1,
	 get_shard_def/1,
	 update_bucket_list/2,
	 get_tab_def/1,
	 delete_table/2,
	 delete_shards/1,
	 delete_shard/1,
	 reduce_cont/2,
	 cut_kvl_at/2,
	 comparator_to_dir/1,
	 get_ldb_worker_args/2,
	 update_table_attr/3,
	 delete_obsolete_indices/3]).

%% Inter-Node API
-export([do_create_shards/1,
	 do_open_table/1,
	 do_close_table/1,
	 do_delete_table/1,
	 do_update_table_attr/3]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb_internal.hrl").

%%%===================================================================
%%% API
%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get the database's paths from configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_path(Path :: db_path |
		       backup_path |
		       wal_path |
		       checkpoint_path |
		       systab_path ) -> string().
get_path(Path) ->
    get_path(Path, undefined).

get_path(Path, Default) ->
    CONF_PATH = gb_conf:get_param("enterdb.yaml", Path),
    case CONF_PATH of
	[$/|_] -> CONF_PATH;
	undefined -> Default;
	_ -> filename:join(gb_conf_env:proddir(), CONF_PATH)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get the configured default for number of local shards.
%% @end
%%--------------------------------------------------------------------
-spec get_num_of_local_shards() -> string().
get_num_of_local_shards() ->
    case gb_conf:get_param("enterdb.yaml", num_of_local_shards) of
	undefined ->
	    ?debug("num_of_local_shards not configured!", []),
            erlang:system_info(schedulers);
	Int when is_integer(Int) ->
	    Int;
	IntStr ->
            case catch list_to_integer(IntStr) of
                Int when is_integer(Int) ->
                    Int;
                _ ->
                    erlang:system_info(schedulers)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Verify the args given to enterdb:create_table/5
%% @end
%%--------------------------------------------------------------------
-spec verify_create_table_args(Args :: [{atom(), term()}]) ->
    {ok, #{}} |
    {error, Reason::term()}.
verify_create_table_args(Args)->
    verify_create_table_args(Args, #{}).

verify_create_table_args([], #{} = EnterdbTable)->
    {ok, EnterdbTable};
verify_create_table_args([{name, Name} | Rest],
			 #{} = EdbTab) when is_list(Name)->
    case verify_name(Name) of
        ok ->
            verify_create_table_args(Rest, EdbTab#{name => Name});
        {error, Reason} ->
            {error, Reason}
    end;
verify_create_table_args([{key, Key}|Rest], #{} = EdbTab) ->
    case verify_key(Key) of
        ok ->
           verify_create_table_args(Rest, EdbTab#{key => Key});
        {error, Reason} ->
           {error, {Key, Reason}}
    end;
verify_create_table_args([{options, Options} | Rest], #{} = EdbTab)
    when is_list(Options)->
    case verify_table_options(Options) of
        ok ->
	    verify_create_table_args(Rest,
		maps:merge(EdbTab, maps:from_list(Options)));
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

-spec get_column_mapper(Name :: string(),
			DataModel :: kv | array | map) ->
    Mapper :: module().
get_column_mapper(_, kv)->
    undefined;
get_column_mapper(Name, DataModel) when DataModel == array;
					DataModel == map ->
    {ok, Module} = gb_reg:new(Name),
    Module.

%%-------------------------------------------------------------------
%% @doc
%% Get table definition
%% @end
%%-------------------------------------------------------------------
-spec get_tab_def(Tab :: string()) ->
    #{} | {error, Reason::term()}.
get_tab_def(Tab) ->
    case mnesia:dirty_read(enterdb_table, Tab) of
	[#enterdb_table{map=TabDef}] ->
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
	[#enterdb_stab{map=ShardTab}] ->
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
-spec update_bucket_list(Shard :: shard_name(),
			 Buckets :: [shard_name()]) ->
    ok | {error, Reason :: term()}.
update_bucket_list(Shard, Buckets) ->
    Fun =
	fun() ->
	    [S = #enterdb_stab{map=Map}] = mnesia:read(enterdb_stab, Shard),
	    NewMap = Map#{buckets=>Buckets},
	    mnesia:write(S#enterdb_stab{map = NewMap})
	end,
    case enterdb_db:transaction(Fun) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

-spec verify_table_options(Options::[table_option()]) ->
    ok | {error, Reason::term()}.
%% Pre configured clusters
verify_table_options([{clusters, Clusters}|Rest])
when is_list(Clusters) ->
    verify_table_options(Rest);

%% Number of Shards
verify_table_options([{num_of_shards, NumOfShards}|Rest])
when is_integer(NumOfShards), NumOfShards > 0 ->
    verify_table_options(Rest);

%% Distributed
verify_table_options([{distributed, Bool}|Rest])
when is_boolean(Bool) ->
    verify_table_options(Rest);

%% System Table
verify_table_options([{system_table, Bool}|Rest])
when is_boolean(Bool) ->
    verify_table_options(Rest);

%% Replication Factor
verify_table_options([{replication_factor, RF}|Rest])
when is_integer(RF), RF > 0 ->
    verify_table_options(Rest);

%% Table types
verify_table_options([{type, Type}|Rest])
    when
	 Type =:= rocksdb
    ->
	verify_table_options(Rest);

%% Data Model
verify_table_options([{data_model, DM}|Rest])
    when
	DM == kv;
        DM == array;
        DM == map
    ->
	verify_table_options(Rest);


%% TTL details for rocksds parts
verify_table_options([{ttl, TTL} | Rest]) when is_integer(TTL) ->
    verify_table_options(Rest);

%% comparator defines how the keys will be sorted
verify_table_options([{comparator, C}|Rest]) when C == descending;
						  C == ascending ->
    verify_table_options(Rest);
%% time_series states the key is compound and contains a timestamp
%% These keys will be hashed without but sorted with timestamp value
verify_table_options([{time_series, T}|Rest]) when is_boolean(T) ->
    verify_table_options(Rest);
%% hash exclude gets a list of key fields to be excluded from hash
%% function. It is a generic alternative to time_series.
verify_table_options([{hash_exclude, L}|Rest]) when is_list(L) ->
    case verify_fields(L) of
        ok ->
	    verify_table_options(Rest);
        {error, Reason} ->
	    {error, Reason}
    end;

%% hash method specifies which strategy to be used by gb_hash app.
verify_table_options([{hashing_method, M}|Rest]) when M == virtual_nodes;
						      M == cosistent;
						      M == uniform;
						      M == rendezvous ->
    verify_table_options(Rest);

%% Raw Rocksdb Options [{"option_name", "option_value"}]
verify_table_options([{rdb_raw, List} | Rest]) when is_list(List) ->
    verify_table_options(Rest);

%% Bad Option
verify_table_options([Elem|_])->
    {error, {Elem, "invalid_option"}};
%% All Options OK
verify_table_options([]) ->
    ok.

-spec check_if_table_exists(Name :: string()) ->
    ok | {error, Reason :: term()}.
check_if_table_exists(Name)->
    case gb_hash:exists(Name) of
	false ->
	    ok;
	true ->
	    {error, "table_exists"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check response for error
%% @end
%%--------------------------------------------------------------------
-spec check_error_response(RespList :: [term()]) ->
    ok | {error, RespList :: [term()]}.
check_error_response([ok]) ->
    ok;
check_error_response(ResponseList) ->
    {error, ResponseList}.

%%--------------------------------------------------------------------
%% @doc
%% Create and return list of {Shard, Ring} tuples for all shards
%% @end
%%--------------------------------------------------------------------
-spec get_shards(Name :: string(),
		 NumOfShards :: pos_integer(),
		 ReplicationFactor :: pos_integer()) ->
    {ok, [{Shard :: string(), Ring :: map()}]}.
get_shards(Name, NumOfShards, ReplicationFactor) ->
    Shards = [lists:concat([Name, "_shard", N])
		|| N <- lists:seq(0, NumOfShards-1)],
    gb_dyno_ring:allocate_nodes(Shards, ReplicationFactor).

%%--------------------------------------------------------------------
%% @doc
%% Extract per Node view from shard allocation.
%% @end
%%--------------------------------------------------------------------
-spec get_allocated_nodes(Shards :: [{Shard :: string(), Ring :: map()}]) ->
    {ok, map()}.
get_allocated_nodes(Shards) ->
    get_allocated_nodes(Shards, #{}).

get_allocated_nodes([{Shard, Ring} | Rest], Acc) ->
    Cont = maps:fold(fun (_, NodeList, AccIn) ->
			map_nodes_to_shards(Shard, NodeList, AccIn)
		     end, Acc, Ring),
    get_allocated_nodes(Rest, Cont);
get_allocated_nodes([], Acc) ->
    {ok, Acc}.

map_nodes_to_shards(Shard, [Node|Rest], Acc) ->
    Cont = maps:update_with(Node, fun(L) -> [Shard|L] end, [Shard], Acc),
    map_nodes_to_shards(Shard, Rest, Cont);
map_nodes_to_shards(_Shard, [], Acc) ->
    Acc.

%%--------------------------------------------------------------------
%% @doc
%% Create and return list of {Shard, Ring} tuples for all shards
%% only for local node.
%% @end
%%--------------------------------------------------------------------
-spec get_local_shards(Name :: string(),
		       NumOfShards :: pos_integer()) ->
    {ok, [Shard :: string()]}.
get_local_shards(Name, NumOfShards) ->
    {ok, [lists:concat([Name, "_shard", N])
	    || N <- lists:seq(0, NumOfShards-1)]}.

%%--------------------------------------------------------------------
%% @doc
%% Call create table (shard) for each shard
%% @end
%%--------------------------------------------------------------------
-spec create_table(EnterdbTable::#{}) ->
    ok | {error, Reason::term()}.
create_table(#{name := Name,
	       distributed := false} = EnterdbTable)->
    NumOfShards	= maps:get(num_of_shards, EnterdbTable),
    %%Generate Shards and allocate nodes on shards
    {ok, Shards} = get_local_shards(Name, NumOfShards),

    %%Create local ring with given allocated shards
    Strategy = maps:get(hashing_method, EnterdbTable),
    HashOpts = [local, {algorithm, sha256}, {strategy, Strategy}],
    {ok, _Beam} = gb_hash:create_ring(Name, Shards, HashOpts),
    do_create_shards(EnterdbTable#{shards => Shards,
				   nodes => [node()]});

create_table(#{name := Name} = EnterdbTable)->
    NumOfShards	= maps:get(num_of_shards, EnterdbTable),
    RF = maps:get(replication_factor, EnterdbTable),

    %%Generate Shards and allocate nodes on shards
    {ok, AllocatedShards} = get_shards(Name, NumOfShards, RF),
    ?debug("table allocated shards ~p", [AllocatedShards]),

    %%Create local ring with given allocated shards
    Strategy = maps:get(hashing_method, EnterdbTable),
    HashOpts = [{algorithm, sha256}, {strategy, Strategy}],
    {ok, Beam} = gb_hash:create_ring(Name, AllocatedShards, HashOpts),

    %% Distribute the ring
    MFA = {gb_hash_register, load_store_ok, [Beam]},
    CommitID = undefined,
    RMFA = {gb_hash_register, revert, [CommitID]},
    Result = ?dyno:topo_call(MFA, [{timeout, 10000}, {revert, RMFA}]),
    {ok, AllocatedNodes} = get_allocated_nodes(AllocatedShards),
    create_table(Result, EnterdbTable#{shards => AllocatedShards,
				       nodes => AllocatedNodes}).

-spec create_table(RingResult :: ok | {error, Reason :: term()},
		   EnterdbTable :: #{}) ->
    ok | {error, Reason::term()}.
create_table(ok, EnterdbTable) ->
    %% Create shards on nodes
    MFA = {?MODULE, do_create_shards, [EnterdbTable]},
    RMFA = {?MODULE, do_delete_table, [maps:get(name, EnterdbTable)]},
    ?dyno:topo_call(MFA, [{timeout, 30000}, {revert, RMFA}]);
create_table({error, Reason}, _EnterdbTable) ->
    ?debug("Create Table failed: ~p", [{error, Reason}]),
    {error, Reason}.

%%--------------------------------------------------------------------
%% @doc
%% Creating shards on local node.
%% @end
%%--------------------------------------------------------------------
-spec do_create_shards(EDBT :: #{}) ->
    ok | {error, Reason :: term()}.
do_create_shards(#{name := Name,
		   data_model := DataModel,
		   shards := Shards} = EDBT) ->
    LocalShards = find_local_shards(Shards),
    Mapper = get_column_mapper(Name, DataModel),
    NewEDBT = EDBT#{column_mapper => Mapper},
    write_enterdb_table(NewEDBT),
    ResL = [do_create_shard(Shard, NewEDBT) || Shard <- LocalShards],
    check_error_response(lists:usort(ResL)).

%%--------------------------------------------------------------------
%% @doc
%% Creating shard on local node.
%% @end
%%--------------------------------------------------------------------
-spec do_create_shard(Shard :: shard_name(),
		      EDBT :: #{}) ->
    ok | {error, Reason :: term()}.
do_create_shard(Shard, EDBT) ->
    DbPath =
	case maps:get(system_table, EDBT, false) of
	    false -> get_path(db_path);
	    true -> get_path(systab_path)
	end,
    Shards = maps:get(shards, EDBT),
    EDBT1 = maps:remove(shards, EDBT),
    ShardDist = proplists:get_value(Shard, Shards),
    EDBT2 = maps:remove(system_table, EDBT1),
    ESTAB = EDBT2#{shard => Shard, shard_dist => ShardDist,
		  db_path => DbPath},
    write_shard_table(ESTAB),
    do_create_shard_type(ESTAB).

do_create_shard_type(#{type := rocksdb} = ESTAB) ->
    create_rocksdb_shard(ESTAB).

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
        {atomic, [#enterdb_stab{map = Map}]} ->
	     do_open_shard(Map);
	{error, Reason} ->
            {error, Reason}
    end.

%% Open existing shard locally

do_open_shard(#{shard := Shard, name := Name} = EDST) ->
    Dist = maps:get(distributed, EDST, false),

    TabMap = get_tab_def(Name),
    SysT = maps:get(system_table, TabMap, false),

    case {Dist, SysT} of
	  {true, false} ->
	    enterdb_recovery:check_ready_status(EDST);
	_ ->
	    ok
    end,
    case enterdb_ns:get(Shard) of
	{error,no_ns_entry} ->
	    do_open_shard_(EDST);
	_Pid ->
	    ?debug("Shard: ~p is already open, ~p.",[Shard, _Pid])
    end.

%% Open existing shard locally
do_open_shard_(#{type := rocksdb} = EDBT) ->
    open_rocksdb_shard(EDBT);
do_open_shard_(Else)->
    ?debug("enterdb:close_table: {type, ~p} not supported", [Else]),
    {error, "type_not_supported"}.

-spec close_shard(Shard :: shard_name()) ->
    ok | {error, Reason :: term()}.
close_shard(Shard) ->
    case enterdb_db:transaction(fun()-> mnesia:read(enterdb_stab, Shard) end) of
        {atomic, []} ->
            {error, "no_table"};
        {atomic, [#enterdb_stab{map = Map}]} ->
	     do_close_shard(Map);
	{error, Reason} ->
            {error, Reason}
    end.

-spec do_close_shard(ESTAB :: #{}) ->
    ok.
do_close_shard(#{shard := Shard} = ESTAB)->
    do_close_shard(ESTAB, enterdb_ns:get(Shard)).

-spec do_close_shard(ESTAB :: #{},
		     Pid :: pid | {error, no_ns_entry}) ->
    ok.
do_close_shard(#{shard := Shard}, {error,no_ns_entry})->
    ?debug("Shard ~p, is already closed.",[Shard]);
do_close_shard(#{type := rocksdb}, Pid)->
    supervisor:terminate_child(enterdb_rdb_sup, Pid);
do_close_shard(Else, _)->
    ?debug("do_close_shard ~p not supported", [Else]),
    {error, "type_not_supported"}.

%% create rocksdb shard
%% TODO: move out to rocksdb specific lib.
-spec create_rocksdb_shard(ESTAB :: #{}) -> ok.
create_rocksdb_shard(ESTAB) ->
    ChildArgs = get_rdb_worker_args(create, ESTAB),
    {ok, _Pid} = supervisor:start_child(enterdb_rdb_sup, [ChildArgs]),
    ok.

%% open rocksdb shard
%% TODO: move out to rocksdb specific lib.
-spec open_rocksdb_shard(ESTAB :: #{}) -> ok.
open_rocksdb_shard(ESTAB) ->
    ChildArgs = get_rdb_worker_args(open, ESTAB),
    {ok, _Pid} = supervisor:start_child(enterdb_rdb_sup, [ChildArgs]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_stab entry in mnesia disc_copy
%% @end
%%--------------------------------------------------------------------
-spec write_shard_table(ShardMap :: #{}) ->
    ok | {error, Reason :: term()}.
write_shard_table(#{shard := Shard} = Map) ->
    Rec = #enterdb_stab{shard = Shard, map = Map},
    case enterdb_db:transaction(fun() -> mnesia:write(Rec) end) of
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
-spec write_enterdb_table(Map :: #{}) ->
    ok | {error, Reason :: term()}.
write_enterdb_table(#{name := Name} = Map) ->
    Rec = #enterdb_table{name = Name, map = Map},
    case enterdb_db:transaction(fun() -> mnesia:write(Rec) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Open an existing database table specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Name :: string(), Dist :: boolean())->
    ok | {error, Reason :: term()}.
open_table(Name, true) ->
    %% Open shards on nodes
    MFA = {?MODULE, do_open_table, [Name]},
    %%RMFA = {?MODULE, do_close_table, [Name]},
    %%?dyno:topo_call(MFA, [{timeout, 60000}, {revert, RMFA}]);
    ?dyno:topo_call(MFA, [{timeout, 60000}]);
open_table(Name, false) ->
    do_open_table(Name).

%%--------------------------------------------------------------------
%% @doc
%% This function is used in inter-node communication.
%% Open database table on local node.
%% @end
%%--------------------------------------------------------------------
-spec do_open_table(Name :: string()) ->
    ok | {error, Reason :: term()}.
do_open_table(Name) ->
    ?info("Opening table: ~p", [Name]),
    case gb_hash:get_nodes(Name) of
	{ok, Shards} ->
	    LocalShards = find_local_shards(Shards),
	    open_shards(LocalShards);
	undefined ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
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
%% Close an existing database table specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Name :: string(), Dist :: boolean()) ->
    ok | {error, Reason :: term()}.
close_table(Name, true) ->
    %% Open shards on nodes
    MFA = {?MODULE, do_close_table, [Name]},
    %%RMFA = {?MODULE, do_open_table, [Name]},
    %%?dyno:topo_call(MFA, [{timeout, 10000}, {revert, RMFA}]);
    ?dyno:topo_call(MFA, [{timeout, 10000}]);
close_table(Name, false) ->
    do_close_table(Name).

%%--------------------------------------------------------------------
%% @doc
%% This function is used in inter-node communication.
%% Close database table on local node.
%% @end
%%--------------------------------------------------------------------
-spec do_close_table(Name :: string()) ->
    ok | {error, Reason :: term()}.
do_close_table(Name) ->
    case gb_hash:get_nodes(Name) of
	{ok, Shards} ->
	    LocalShards = find_local_shards(Shards),
	    close_shards(LocalShards);
	undefined ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Close database table shards on defined node.
%% @end
%%--------------------------------------------------------------------
-spec close_shards(ShardList :: [string()]) ->
    ok | {error, Reason :: term()}.
close_shards([]) ->
    ok;
close_shards([Shard | Rest]) ->
    ?debug("Closing Shard: ~p",[Shard]),
    case close_shard(Shard) of
	ok ->
	    close_shards(Rest);
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete an existing database table specified by Name.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Name :: string(), Dist :: boolean()) ->
    ok | {error, Reason :: term()}.
delete_table(Name, true) ->
    %% Open shards on nodes
    MFA = {?MODULE, do_delete_table, [Name]},
    RMFA = undefined,
    ?dyno:topo_call(MFA, [{timeout, 10000}, {revert, RMFA}]);
delete_table(Name, false) ->
    do_delete_table(Name).

-spec do_delete_table(Name :: string()) ->
ok | {error, Reason :: term()}.
do_delete_table(Name) ->
    case gb_hash:get_nodes(Name) of
	{ok, Shards} ->
	    LocalShards = find_local_shards(Shards),
	    ?info("Deleting ~p, Local Shards: ~p",[Name, LocalShards]),
	    delete_shards(LocalShards),
	    cleanup_table(Name);
	undefined ->
	    {error, "no_table"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete an existing table shards.
%% This function should be called within a mnesia transaction.
%% @end
%%--------------------------------------------------------------------
-spec delete_shards([Shard :: string()]) ->
    ok | {error, Reason :: term()}.
delete_shards([Shard | Rest]) ->
    delete_shard(Shard),
    delete_shards(Rest);
delete_shards([]) ->
    ok.

delete_shard(Shard) ->
    try
	SD = get_shard_def(Shard),
	?debug("Deleting ~p, SD: ~p",[Shard, SD]),
	mnesia:dirty_delete(enterdb_stab, Shard),
	delete_shard_help(SD),
	do_close_shard(SD)
    catch _:_ ->
	?info("could not close shard ~p", [Shard])
    end.

%% add delete per type
delete_shard_help(ESTAB = #{type := rocksdb}) ->
    Args = get_rdb_worker_args(delete, ESTAB),
    enterdb_rdb_worker:delete_db(Args);
delete_shard_help({error, Reason}) ->
    {error, Reason}.

cleanup_table(Name) ->
    case get_tab_def(Name) of
	{error,"no_table"} ->
	    ok;
	#{name := Name,
	  column_mapper := ColumnMapper} ->
	    Path = get_path(db_path),
	    FullPath = filename:join([Path, Name]),
	    file:del_dir(FullPath),
	    gb_reg:purge(ColumnMapper),
	    mnesia:dirty_delete(enterdb_table, Name)
    end,
    gb_hash:delete_ring(Name).

%%--------------------------------------------------------------------
%% @doc
%% Reads a Range of Keys from table Tab from Shards and returns max
%% Chunk items.
%% @end
%%--------------------------------------------------------------------
-spec read_range_on_shards({ok, Shards :: shards()} | undefined,
			   Tab :: #{},
			   {StartKey :: binary(), StopKey :: binary()},
			   Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont :: complete | key()} | {error, Reason :: term()}.
read_range_on_shards({ok, Shards},
		     Tab = #{key := KeyDef,
			     type := Type,
			     comparator := Comp,
			     distributed := Dist},
		     RangeDB, Chunk)->
    Dir = comparator_to_dir(Comp),
    {CallbackMod, TrailingArgs} =
	case Type of
	    rocksdb -> {enterdb_rdb_worker, []}
	end,
    BaseArgs = [RangeDB, Chunk | TrailingArgs],
    Req = {CallbackMod, read_range_binary, BaseArgs},
    ResL = map_shards_seq(Dist, Req, Shards),
    {KVLs, Conts} =  unzip_range_result(ResL, []),
    ContKeys = [K || K <- Conts, K =/= complete],
    {ok, KVL, ContKey} = merge_and_cut_kvls(Dir, KeyDef, KVLs, ContKeys),
    {ok, ResultKVL} = make_app_kvp(Tab, KVL),
    {ok, ResultKVL, ContKey}.

-spec map_shards_seq(Dist :: true | false,
		     Req :: {module(), function(), [term()]},
		     Shards :: shards()) ->
    ResL :: [term()].
map_shards_seq(true, Req, Shards) ->
    ?dyno:map_shards_seq(Req, Shards);
map_shards_seq(false, Req, Shards) ->
    pmap(Req, Shards).

-spec map_shards(Dist :: true | false,
		 Req :: {module(), function(), [term()]},
		 Mode :: write | read,
		 Shards :: shards()) ->
    ResL :: [term()].
map_shards(true, Req, Mode, Shards) ->
    ?dyno:map_shards(Req, Mode, Shards);
map_shards(false, Req, _Mode, Shards) ->
    pmap(Req, Shards).

-spec unzip_range_result(ResL :: [{ok, KVL :: [kvp()], Cont :: term()}],
			 Acc :: {[[kvp()]], [term()]}) ->
     {KVLs :: [KVL :: [kvp()]],
      Conts :: [term()]}.
unzip_range_result([{ok, KVL, Cont} | Rest], Acc) ->
    unzip_range_result(Rest, [{KVL, Cont} | Acc]);
unzip_range_result([Error | _Rest], _Acc) ->
    Error;
unzip_range_result([], Acc) ->
    lists:unzip(lists:reverse(Acc)).



-spec merge_and_cut_kvls(Dir :: 0 | 1,
			 KeyDef :: [string()],
			 KVLs :: [[kvp()]],
			 ContKeys :: [binary()]) ->
    {ok, KVL :: [kvp()]}.
merge_and_cut_kvls(Dir, _KeyDef, KVLs, []) ->
   {ok, KVL} = enterdb_utils:merge_sorted_kvls(Dir, KVLs),
   {ok, KVL, complete};
merge_and_cut_kvls(Dir, KeyDef, KVLs, ContKeys) ->
    {Cont, _} = ContKVP = reduce_cont(Dir, ContKeys),
    {ok, MergedKVL} = enterdb_utils:merge_sorted_kvls(Dir, [[ContKVP]|KVLs]),
    ContKey =  make_app_key(KeyDef, Cont),
    {ok, cut_kvl_at(Cont, MergedKVL), ContKey}.

-spec reduce_cont(Comparator :: comparator(),
		  Conts :: [binary()]) ->
    {key(), binary()}.
reduce_cont(Dir, ContKeys) ->
    SortableKVPs = [{K, <<>>} || K <- ContKeys],
    {ok, Sorted} = enterdb_utils:sort_kvl( Dir, SortableKVPs ),
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
%% that is given by Ring and merges collected key/value lists.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n_on_shards({ok, Shards :: shards()} | undefined,
			     Tab :: #{},
			     DBStartKey :: binary(),
			     N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n_on_shards(undefined, _Tab, _DBStartKey, _N) ->
     {error, "no_table"};
read_range_n_on_shards({ok, Shards},
		       Tab = #{type := Type,
			       comparator := Comp,
			       distributed := Dist},
		       DBStartKey, N) ->
    ?debug("DBStartKey: ~p, Shards: ~p",[DBStartKey, Shards]),
    Dir = comparator_to_dir(Comp),
    {CallbackMod, TrailingArgs} =
	case Type of
	    rocksdb -> {enterdb_rdb_worker, []}
	end,
    %%To be more efficient we can read less number of records from each shard.
    %%NofShards = length(Shards),
    %%Part = (N div NofShards) + 1,
    %%To be safe, currently we try to read N from each shard.
    BaseArgs = [DBStartKey, N | TrailingArgs],
    Req = {CallbackMod, read_range_n_binary, BaseArgs},
    ResL = map_shards_seq(Dist, Req, Shards),
    KVLs = [begin {ok, R} = Res, R end || Res <- ResL],
    {ok, MergedKVL} = enterdb_utils:merge_sorted_kvls(Dir, KVLs),
    N_KVP = lists:sublist(MergedKVL, N),
    make_app_kvp(Tab, N_KVP).

%%--------------------------------------------------------------------
%% @doc
%% Get byte size from each shard of a table and return the sum.
%% @end
%%--------------------------------------------------------------------
-spec approximate_size(Backend :: string(),
		       Shards :: shards(),
		       Dist :: boolean()) ->
    {ok, Size :: pos_integer()} | {error, Reason :: term()}.
approximate_size(rocksdb, Shards, true)  ->
    Req = {enterdb_rdb_worker, approximate_size, []},
    Sizes = ?dyno:map_shards_seq(Req, Shards),
    ?debug("Sizes of all shards: ~p", [Sizes]),
    sum_up_sizes(Sizes, 0);
approximate_size(rocksdb, Shards, false) ->
    Req = {enterdb_rdb_worker, approximate_size, []},
    Sizes = pmap(Req, Shards),
    ?debug("Sizes of all shards: ~p", [Sizes]),
    sum_up_sizes(Sizes, 0);
approximate_size(Type, _, _) ->
    ?debug("Size approximation is not supported for type: ~p", [Type]),
    {error, "type_not_supported"}.

-spec sum_up_sizes(Sizes :: [{ok, integer()} | {error, Reason :: term()}],
		   Sum :: pos_integer()) ->
    {ok, Size :: pos_integer()}.
sum_up_sizes([], Sum) ->
    {ok, Sum};
sum_up_sizes([{ok, Int} | Rest], Sum) when is_integer(Int) ->
    sum_up_sizes(Rest, Sum + Int);
sum_up_sizes([_ | Rest], Sum) ->
    sum_up_sizes(Rest, Sum).

%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration.
%% @end
%%--------------------------------------------------------------------
-spec make_key(TD :: #{},
	       Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary(), HashKey :: binary()} |
    {error, Reason :: term()}.
make_key(#{key := KeyDef, hash_key := HashKey}, Key) ->
    make_db_key(KeyDef, HashKey, Key).

%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and also
%% list of binary encoded cids(column ids) of indexed columns.
%% @end
%%--------------------------------------------------------------------
-spec make_key_cids(TableDef :: #{},
		    Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary(), HashKey :: binary(), Cids :: [binary()]} |
    {error, Reason :: term()}.
make_key_cids(#{key := KeyDef,
		hash_key := HashKeyDef,
		column_mapper := ColumnMapper,
		index_on := IndexOn}, Key) ->
    case make_db_key(KeyDef, HashKeyDef, Key) of
	{ok, DBKey, HashKey} ->
	    Cids = get_indexed_cids(ColumnMapper, IndexOn),
	    {ok, DBKey, HashKey, Cids};
	{error, E} ->
	    {error, E}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and also
%% columns according to DataModel and Column Mapper.
%% @end
%%--------------------------------------------------------------------
-spec make_key_columns(TableDef :: #{},
		       Key :: [{string(), term()}],
		       Columns :: term()) ->
    {ok, DbKey :: binary(), HashKey :: binary(),
	 Columns :: binary(), IndexTerms :: [string()]} |
    {error, Reason :: term()}.
make_key_columns(TD = #{key := KeyDef,
			hash_key := HashKeyDef}, Key, Columns) ->
    case make_db_key(KeyDef, HashKeyDef, Key) of
	{ok, DBKey, HashKey} ->
	    make_key_columns_help(TD, DBKey, HashKey, Columns);
	{error, E} ->
	    {error, E}
    end.

make_key_columns_help(#{data_model := DataModel,
			column_mapper := ColumnMapper,
			distributed := Dist,
			index_on := IndexOn},
		      DBKey,
		      HashKey,
		      Columns) ->
    case make_db_value(DataModel, ColumnMapper, Dist, Columns) of
	{ok, DBValue} ->
	    IndexTerms = get_index_terms(ColumnMapper, IndexOn, Columns),
	    %%io:format("~p:~p, index_terms ~p~n",[?MODULE,?LINE, IndexTerms]),
	    {ok, DBKey, HashKey, DBValue, IndexTerms};
	{error, E} ->
	    {error, E}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and
%% provided values in Key. Return DBKey which is stored.
%% @end
%%--------------------------------------------------------------------
-spec make_db_key(KeyDef :: [string()],
		  Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary()} | {error, Reason :: term()}.
make_db_key(KeyDef, Key) ->
    case make_db_key(KeyDef, [], Key) of
	{ok, DbKey, _} -> {ok, DbKey};
	Else -> Else
    end.

%%-------------------------------------------------------------------
%% @doc
%% Make key according to KeyDef defined in table configuration and
%% provided values in Key. Return both DBKey which is stored and
%% HashKey which is used in hash function to locate the shard that
%% stores DBKey.
%% @end
%%--------------------------------------------------------------------
-spec make_db_key(KeyDef :: [string()],
		  HashKeyDef :: [string()],
		  Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary(), HashKey :: binary()} |
    {error, Reason :: term()}.
make_db_key(KeyDef, HashKeyDef, Key) ->
    KeyDefLen = length(KeyDef),
    KeyLen = length(Key),
    if KeyDefLen == KeyLen ->
	make_db_key(KeyDef, HashKeyDef, Key, [], []);
       true ->
        {error, "key_mismatch"}
    end.

-spec make_db_key(KeyDef :: [string()],
		  HashKeyDef :: [string()],
		  Key :: [{string(), term()}],
		  DBKeyList :: [term()],
		  HashKeyList :: [term()]) ->
    {ok, DBKey :: binary(), HashKey :: binary()} |
    {error, Reason::term()}.
make_db_key([Field | RestD], [Field | RestH], Key, DBKeyList, HashKeyList) ->
    case lists:keyfind(Field, 1, Key) of
        {_, Val} ->
            make_db_key(RestD, RestH, Key, [Val|DBKeyList], [Val|HashKeyList]);
        false ->
            {error, "key_mismatch"}
    end;
make_db_key([Field | RestD], RestH, Key, DBKeyList, HashKeyList) ->
    case lists:keyfind(Field, 1, Key) of
        {_, Val} ->
            make_db_key(RestD, RestH, Key, [Val|DBKeyList], HashKeyList);
        false ->
            {error, "key_mismatch"}
    end;
make_db_key([], _, _, DBKeyList, HashKeyList) ->
    TupleD = list_to_tuple(lists:reverse(DBKeyList)),
    {ok, sext:encode(TupleD), sext:encode(HashKeyList)}.

%%--------------------------------------------------------------------
%% @doc
%% Make DB value according to DataModel and Columns Definition that is
%% in table configuration and provided values in Columns.
%% @end
%%--------------------------------------------------------------------
-spec make_db_value(DataModel :: data_model(),
		    ColumnMApper :: module(),
		    Distributed :: boolean(),
		    Columns :: [{string(), term()}])->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_value(kv, _, _, Columns) ->
    {ok, term_to_binary(Columns)};
make_db_value(array, ColumnMapper, Distributed, Columns) ->
    make_db_array_value(ColumnMapper, Distributed, Columns);
make_db_value(map, ColumnMapper, Distributed, Columns) ->
    make_db_map_value(ColumnMapper, Distributed, Columns).

-spec make_db_array_value(Mapper :: module(),
			  Distributed :: boolean(),
			  Columns :: [{string(), term()}]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_array_value(Mapper, Distributed, Columns) ->
    Array = serialize_columns(Mapper, Distributed, Columns, 0, []),
    {ok, term_to_binary(Array)}.

-spec make_db_map_value(Mapper :: module(),
		        Distributed :: boolean(),
			Columns :: [{string(), term()}]) ->
    {ok, DbValue :: binary()} | {error, Reason :: term()}.
make_db_map_value(Mapper, Distributed, Columns) ->
    Map = map_columns(Mapper, Distributed, Columns, []),
    {ok, term_to_binary(Map)}.

-spec serialize_columns(Mapper :: module(),
			Distributed :: boolean(),
			Columns :: [{string(), term()}],
			Ref :: integer(),
			Acc :: [binary()]) ->
    [term()].
serialize_columns(Mapper, Distributed, Columns, Ref, Acc) ->
    case Mapper:lookup(Ref) of
	undefined ->
	    add_columns(Mapper, Distributed, Columns, Ref, Acc);
	Field ->
	    case lists:keytake(Field, 1, Columns) of
		{_, {_,Value}, Rest} ->
		    NewRef= Ref+1,
		    serialize_columns(Mapper, Distributed, Rest,
				      NewRef, [{NewRef, Value} | Acc]);
		false ->
		    serialize_columns(Mapper, Distributed, Columns, Ref+1, Acc)
	    end
    end.

-spec add_columns(Mapper :: module(),
		  Distributed :: boolean(),
		  Columns :: [{string(), term()}],
		  Ref :: integer(),
		  Acc :: [{integer(), term()}]) ->
    {Arity :: integer(), InitList :: [{integer(), term()}]}.
add_columns(_Mapper, _Distributed, [], _Ref, []) ->
    {};
add_columns(_Mapper, _Distributed, [], _Ref, [{Arity,_}|_] = Acc) ->
    erlang:make_tuple(Arity, 'NULL', Acc);
add_columns(Mapper, Distributed, Columns, Ref, Acc) ->
    AddKeys = [Field || {Field, _} <- Columns],
    case Distributed of
	true ->
	    ?dyno:topo_call({gb_reg, add_keys, [Mapper, AddKeys]},
		    [{timeout, 10000}]);
	false ->
	    gb_reg:add_keys(Mapper, AddKeys)
    end,
    serialize_columns(Mapper, Distributed, Columns, Ref, Acc).

-spec map_columns(Mapper :: module(),
		  Distributed :: boolean(),
		  Columns :: [{string(), term()}],
		  Acc :: [binary()]) ->
    [term()].
map_columns(Mapper, Distributed, [{Field, Value} | Rest], Acc) ->
    case Mapper:lookup(Field) of
	undefined ->
	    map_columns(Mapper, Distributed, Rest,
			[{'$no_mapping', Field, Value} | Acc]);
	Ref ->
	    Bin = binary:encode_unsigned(Ref, big),
	    map_columns(Mapper, Distributed, Rest, [Bin, Value | Acc])
    end;
map_columns(Mapper, Distributed, [], Acc) ->
    case [Field || {'$no_mapping', Field, _} <- Acc] of
	[] ->
	    Acc;
	AddKeys ->
	    Rest = [{Field, Value} || {'$no_mapping', Field, Value} <- Acc],
	    Done = lists:filter(fun({'$no_mapping',_,_}) -> false;
				   (_) -> true
				end, Acc),
	    case Distributed of
		true ->
		    ?dyno:topo_call({gb_reg, add_keys, [Mapper, AddKeys]},
				    [{timeout, 10000}]);
		false ->
		    gb_reg:add_keys(Mapper, AddKeys)
	    end,
	    map_columns(Mapper, Distributed, Rest, Done)
    end.

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
    lists:zip(KeyDef, tuple_to_list(sext:decode(DbKey))).

%%--------------------------------------------------------------------
%% @doc
%% Make application value according to Columns Definition defined in
%% table configuration and DB Value.
%% Takes internal record #enterdb_stab{} as argument carrying model and
%% columns definitions.
%% @end
%%--------------------------------------------------------------------
-spec make_app_value(TD :: #{},
		     DBValue :: {ok, binary()} | {error, Reason::term()})->
    Columns :: [term()].
make_app_value(_TD, {error, R}) ->
    {error, R};
make_app_value(TD, {ok, DBValue}) ->
    #{data_model := DataModel,
      column_mapper := ColumnMapper} = TD,
    {ok, make_app_value(DataModel, ColumnMapper, DBValue)}.

%%--------------------------------------------------------------------
%% @doc
%% Make application value according to Columns Definition defined in
%% table configuration and DB Value.
%% @end
%%--------------------------------------------------------------------
-spec make_app_value(DataModel :: data_model(),
		     ColumnMapper :: module(),
		     DBValue :: binary()) ->
    Columns :: [term()].
make_app_value(DataModel, Mapper, DBValue) when not is_binary(DBValue)  ->
    format_app_value(DataModel, Mapper, DBValue);
make_app_value(DataModel, Mapper, DBValue) ->
    format_app_value(DataModel, Mapper, binary_to_term(DBValue)).

-spec format_app_value(DataModel :: data_model(),
		       Mapper :: module(),
		       Value :: term()) ->
    Columns :: [{string(), term()}].
format_app_value(kv, _, Value) ->
    Value;
format_app_value(array, Mapper, Columns) ->
    Array = erlang:tuple_to_list(Columns),
    converse_array_columns(Mapper, Array, 0, []);
format_app_value(map, Mapper, Columns) ->
    converse_columns(Mapper, Columns, []).

-spec converse_array_columns(Mapper :: module(),
			     Columns :: [term()],
			     Ref :: integer(),
			     Acc :: [{string(), term()}]) ->
    Columns :: [{string(), term()}].
converse_array_columns(Mapper, ['NULL' | Rest], Ref, Acc) ->
    converse_array_columns(Mapper, Rest, Ref+1, Acc);
converse_array_columns(Mapper, [Value | Rest], Ref, Acc) ->
    Field = Mapper:lookup(Ref),
    converse_array_columns(Mapper, Rest, Ref+1, [{Field, Value} | Acc]);
converse_array_columns(_, [], _Ref, Acc) ->
    lists:reverse(Acc).

-spec converse_columns(Mapper :: module(),
		       Columns :: [term()],
		       Acc :: [binary()]) ->
    Columns :: [{string(), term()}].
converse_columns(Mapper, [Bin, Value | Rest], Acc) ->
    Field = Mapper:lookup(binary:decode_unsigned(Bin, big)),
    converse_columns(Mapper, Rest, [{Field, Value} | Acc]);
converse_columns(_, [], Acc) ->
    Acc.

%%--------------------------------------------------------------------
%% @doc
%% Format a key/value list or key/value pair of binaries
%% according to table's data model.
%% @end
%%--------------------------------------------------------------------
-spec make_app_kvp(Tab :: #{},
		   KVP :: {binary(), binary()} |
			  [{binary(), binary()}]) ->
    {ok, [{key(), value()}]} | {error, Reason :: term()}.
make_app_kvp(#{key := KeyDef,
	       column_mapper := ColumnMapper,
	       data_model := DataModel}, KVP) ->
    make_app_kvp(DataModel, KeyDef, ColumnMapper, KVP).

%%--------------------------------------------------------------------
%% @doc
%% Format a key/value list or key/value pair of binaries
%% according to table's data model.
%% @end
%%--------------------------------------------------------------------
-spec make_app_kvp(DataModel :: data_model(),
		   KeyDef :: [string()],
		   ColumnMapper :: module(),
		   KVP :: {binary(), binary()} |
			  [{binary(), binary()}]) ->
    {ok, [{key(), value()}]} | {error, Reason :: term()}.
make_app_kvp(DataModel, KeyDef, Mapper, KVP) ->
    AppKVP =
	case KVP of
	    [_|_] ->
		[begin
		    K = make_app_key(KeyDef, BK),
		    V = make_app_value(DataModel, Mapper, BV),
		    {K, V}
		 end || {BK, BV} <- KVP];
	    {BinKey, BinValue} ->
		{make_app_key(KeyDef, BinKey),
		 make_app_value(DataModel, Mapper, BinValue)};
	    [] ->
		[];
	    _ ->
		{error, {invalid_arg, KVP}}
	end,
    {ok, AppKVP}.

%%--------------------------------------------------------------------
%% @doc
%% Given update operation, overwrite fields with new values on given
%% DB Value.
%% @end
%%--------------------------------------------------------------------
-spec apply_update_op(Op :: update_op(),
		      DBValue :: binary(),
		      TabSpecs :: map()) ->
    {ok, UpdatedValue :: binary(), IndexTerms :: [string()]}|
    {error, Reason :: term()}.
apply_update_op(Op, DBValue, #{data_model := DataModel,
			       column_mapper := Mapper,
			       distributed := Dist,
			       index_on := IndexOn}) ->
    Columns = make_app_value(DataModel, Mapper, DBValue),
    UpdatedColumns = apply_update_op(Op, Columns),
    case make_db_value(DataModel, Mapper, Dist, UpdatedColumns) of
	{ok, DBValue} ->
	    {ok, DBValue, []};
	{ok, UpdatedDBValue} ->
	    IndexTerms = get_index_terms(Mapper, IndexOn, Columns),
	    {ok, UpdatedDBValue, IndexTerms};
	{error, E} ->
	    {error, E}
    end.

apply_update_op([], UpdatedColumns) ->
    UpdatedColumns;
apply_update_op([{Field, Inst, Data, Default} | Rest], Columns) ->
    case lists:keytake(Field, 1, Columns) of
	false ->
	    Value = {value, {Field, Default}, Columns},
	    UpdatedColumns = apply_instruction(Inst, Data, Value),
	    apply_update_op(Rest, UpdatedColumns);
	Value ->
	    UpdatedColumns = apply_instruction(Inst, Data, Value),
	    apply_update_op(Rest, UpdatedColumns)
    end;
apply_update_op([{Field, Inst, Data} | Rest], Columns) ->
    case lists:keytake(Field, 1, Columns) of
	false ->
	    apply_update_op(Rest, Columns);
	Value ->
	    UpdatedColumns = apply_instruction(Inst, Data, Value),
	    apply_update_op(Rest, UpdatedColumns)
    end;
apply_update_op([{Field, Data} | Rest], Columns) ->
    case lists:keytake(Field, 1, Columns) of
	false ->
	    UpdatedColumns = apply_instruction(overwrite, Data, {a, {Field, a}, Columns}),
	    apply_update_op(Rest, UpdatedColumns);
	Value ->
	    UpdatedColumns = apply_instruction(overwrite, Data, Value),
	    apply_update_op(Rest, UpdatedColumns)
    end.

apply_instruction(increment, I, {_, {F, V}, R}) when is_integer(I) ->
    [{F, V+I} | R];
apply_instruction({increment, T, S}, I, {_, {F, V}, R}) when is_integer(I) ->
    U = V+I,
    if U > T ->
	    [{F, S+(U-T-1)} | R];
	true ->
	    [{F, U} | R]
    end;
apply_instruction(overwrite, Term, {_, {F, _}, R}) ->
    [{F, Term} | R];
apply_instruction(_, _, {_, E, R}) ->
    [E | R].

%%--------------------------------------------------------------------
%% @doc
%% Build the list of key fields those are going to be used in
%% hash function when locating the shard that contains the entry with
%% a given key.
%% @end
%%--------------------------------------------------------------------
-spec get_hash_key_def(KeyDef :: [string()],
		       HashExclude :: [string()],
		       TimeSeries :: boolean()) ->
    HashKey :: [string()].
get_hash_key_def(KeyDef, HashExclude, false) ->
    build_hash_key_def(KeyDef, HashExclude, []);
get_hash_key_def(KeyDef, HashExclude, true) ->
    build_hash_key_def(KeyDef, ["ts" | HashExclude], []).

-spec build_hash_key_def(KeyDef :: [string()],
			 HashExclude :: [string()],
			 Acc :: [string()]) ->
    HashKey :: [string()].
build_hash_key_def([F|Rest], HashExclude, Acc) ->
    case lists:member(F, HashExclude) of
	false ->
	    build_hash_key_def(Rest, HashExclude, [F|Acc]);
	true ->
	    build_hash_key_def(Rest, HashExclude, Acc)
    end;
build_hash_key_def([], _HashExclude, Acc) ->
    lists:reverse(Acc).

-spec comparator_to_dir(Comparator :: descending | ascending) ->
    0 | 1.
comparator_to_dir(descending) ->
    0;
comparator_to_dir(ascending) ->
    1.

-spec find_local_shards(Shards :: shards()) ->
    [Shard :: string()].
find_local_shards([S | _] = Shards) when is_list(S) ->
    Shards;
find_local_shards(Shards) ->
    find_local_shards(Shards, node(), gb_dyno:conf(dc), []).

-spec find_local_shards(Shards :: [{Shard :: string(), Ring :: map()}],
			Node :: node(),
			DC :: string(),
			Acc :: [string()]) ->
    [Shard :: string()].
find_local_shards([{S, Ring} | Rest], Node, DC, Acc) ->
    Nodes = maps:get(DC, Ring, []),
    NewAcc =
	case lists:member(Node, Nodes) of
	    true -> [S | Acc];
	    false -> Acc
	end,
    find_local_shards(Rest, Node, DC, NewAcc);
find_local_shards([], _Node, _DC, Acc) ->
    Acc.

%%--------------------------------------------------------------------
%% @doc
%% Parallel map requests on local node. Args will be constructed by
%% Adding Elements from List to BaseArgs. apply(Mod, Fun, Args)
%% will be called on local node. Result list will be in respective
%% order to request list.
%% @end
%%--------------------------------------------------------------------
-spec pmap({Mod:: module(), Fun :: function(), BaseArgs :: [term()]},
	   List :: [term()]) ->
    ResL :: [Result :: term()].
pmap({Mod, Fun, BaseArgs}, List) ->
    Reqs = [{Mod, Fun, [Elem | BaseArgs]} || Elem <- List],
    peval(Reqs).

%%--------------------------------------------------------------------
%% @doc
%% Parallel evaluate requests on local node. apply(Mod, Fun, Args)
%% will be called on local node. Result list will be in respective
%% to request list.
%% @end
%%--------------------------------------------------------------------
-spec peval( Reqs :: [{module(), function(), [term()]}]) ->
    ResL :: [term()].
peval(Reqs) ->
    ReplyTo = self(),
    Pids = [async_eval(ReplyTo, Req) || Req <- Reqs],
    [yield(P) || P <- Pids].

-spec async_eval(ReplyTo :: pid(),
		 Req :: {module(), function(), [term()]}) ->
    Pid :: pid().
async_eval(ReplyTo, {Mod, Fun, Args}) ->
    spawn(
      fun() ->
	      R = apply(Mod, Fun, Args),
	      ReplyTo ! {self(), {promise_reply, R}}
      end).

-spec yield(Pid :: pid()) ->
    term().
yield(Pid) when is_pid(Pid) ->
    {value, R} = do_yield(Pid, infinity),
    R.

-spec do_yield(Pid :: pid,
	       Timeout :: non_neg_integer() | infinity) ->
    {value, R :: term()} | timeout.
do_yield(Pid, Timeout) ->
    receive
        {Pid, {promise_reply,R}} ->
            {value, R}
        after Timeout ->
            timeout
    end.

-spec get_ldb_worker_args(Start :: create | open | delete,
			  Smap :: #{}) ->
    [term()].
get_ldb_worker_args(Start, Smap = #{shard := Shard,
				    name := Name,
				    comparator := Comparator,
				    db_path := Path}) ->
    Options = [{comparator, Comparator} | ldb_open_options(Start)],
    [{name, Shard}, {db_path, Path}, {subdir, Name}, {options, Options},
     {tab_rec, Smap}].

-spec ldb_open_options(Start :: create | open | delete) ->
    [{create_if_missing, boolean()} |
     {error_if_exists, boolean()}].
ldb_open_options(create) ->
    [{create_if_missing, true}, {error_if_exists, true}];
ldb_open_options(Start) when Start == open; Start == delete ->
    [{create_if_missing, false}, {error_if_exists, false}].

-spec get_rdb_worker_args(Start :: create | open | delete,
			  Smap :: #{}) ->
    map().
get_rdb_worker_args(Start, Smap = #{comparator := Comp}) ->
    RdbRawArgs = maps:get(rdb_raw, Smap, []),
    Options = [{"comparator", cmp_str(Comp)} | rdb_open_options(Start) ++ RdbRawArgs],
    maps:put(options, Options, Smap).

-spec rdb_open_options(Start :: create | open | delete) ->
    [{string(), string()}].
rdb_open_options(create) ->
    [{"create_if_missing", "true"}, {"error_if_exists", "true"},
     {"create_missing_column_families", "true"}];
rdb_open_options(Start) when Start == open; Start == delete ->
    [{"create_if_missing", "false"}, {"error_if_exists", "false"}].

cmp_str(descending) -> "descending";
cmp_str(ascending) -> "ascending".

get_indexed_cids(Mapper, IndexOn) ->
    get_indexed_cids(Mapper, IndexOn, []).

get_indexed_cids(Mapper, [{Col, _} | Rest], Acc) ->
    case Mapper:lookup(Col) of
	undefined ->
	    get_indexed_cids(Mapper, Rest, Acc);
	Ref ->
	    Cid = encode_unsigned(2, Ref),
	    get_indexed_cids(Mapper, Rest, [Cid | Acc])
    end;
get_indexed_cids(_Mapper, [], Acc) ->
    Acc.

get_index_terms(Mapper, IndexOn, Columns) ->
    get_index_terms(Mapper, IndexOn, Columns, []).

get_index_terms(Mapper, [{Col, IndexOptions} | Rest], Columns, Acc) ->
    case lists:keyfind(Col, 1, Columns) of
	{_, Value} when is_list(Value)->
	    Cid = encode_unsigned(2, Mapper:lookup(Col)),
	    Terms = make_index_terms(Value, IndexOptions),
	    BinTerms = string_to_binary_terms(Terms),
	    get_index_terms(Mapper, Rest, Columns, [{Cid, BinTerms} | Acc]);
	_ ->
	    get_index_terms(Mapper, Rest, Columns, Acc)
    end;
get_index_terms(_Mapper, [], _Columns, Acc) ->
    Acc.

-spec update_table_attr(TD :: map(),
			Attr :: atom(),
			Val :: term()) ->
    ok.
update_table_attr(#{name := Name,
		    distributed := false}, Attr, Val) ->
    do_update_table_attr(Name, Attr, Val);
update_table_attr(#{name := Name,
		    nodes := Nodes,
		    distributed := true} = TD, Attr, Val) ->
    MFA = {enterdb_lib, do_update_table_attr, [Name, Attr, Val]},
    OldVal = maps:get(Attr, TD, undefined),
    RMFA = {enterdb_lib, do_update_table_attr, [Name, Attr, OldVal]},
    Options = [{timeout, 30000}, {revert, RMFA}],
    Res = ?dyno:call_nodes(Nodes, MFA, Options),
    ?debug("update_table_attr result: ~p", [Res]),
    Res.

set_attr(TD, Attr, undefined) ->
    maps:remove(Attr, TD);
set_attr(TD, Attr, Val) ->
    maps:put(Attr, Val, TD).

update_table_attr_fun(Name, Attr, Val) ->
    case get_tab_def(Name) of
	#{Attr := Val} ->
	    ok;
	#{name := Name, shards := Shards} = TD ->
	    New = set_attr(TD, Attr, Val),
	    mnesia:write(#enterdb_table{name = Name, map = New}),
	    ResL = update_shard_attrs(Shards, Attr, Val),
	    check_error_response(lists:usort(ResL));
	{error, "no_table"} ->
	    ok
    end.

update_shard_attrs(Shards, Attr, Val) ->
    [begin
	case mnesia:read(enterdb_stab, Shard) of
	    [] ->
		ok;
	    [S = #enterdb_stab{map = M}] ->
		mnesia:write(S#enterdb_stab{map = M#{Attr => Val}})
	end
     end || {Shard, _} <- Shards].

do_update_table_attr(Name, Attr, Val) ->
    Fun = fun update_table_attr_fun/3,
    case mnesia:transaction(Fun, [Name, Attr, Val]) of
	{aborted, Reason} ->
	    {error, Reason};
	{atomic, ok} ->
	    ok
    end.

-spec make_index_terms(Value :: string(),
		       IndexOptions :: index_options()) ->
    [Term :: {unicode:charlist(), integer(), integer()} |
	     {unicode:charlist(), integer()} |
	     unicode:charlist()].
make_index_terms(String, undefined) ->
    [String];
make_index_terms(String, IndexOptions) ->
    term_prep:analyze(IndexOptions, String).

-spec encode_unsigned(Size :: integer(), Int :: integer()) ->
    binary().
encode_unsigned(Size, Int) when is_integer(Int) ->
    Unsigned = binary:encode_unsigned(Int, big),
    case Size - size(Unsigned) of
	Fill when Fill >= 0 ->
	    << <<0:Fill/unit:8>>/binary, Unsigned/binary >>;
	_ ->
	    Unsigned
    end.

-spec string_to_binary_terms([Term :: {string(), integer(), integer()} |
				      {string(), integer()} |
				      string()]) ->
    [binary()].
string_to_binary_terms(Terms) ->
    ?debug("~p:~p(~p)",[?MODULE,string_to_binary_terms,Terms]),
    string_to_binary_terms(Terms, []).

string_to_binary_terms([{Str, F, P} | Rest], Acc) ->
    Bin = list_to_binary(Str),
    FreqBin = encode_unsigned(4, F),
    PosBin = encode_unsigned(4, P),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    FreqBin/binary,
				    PosBin/binary>> | Acc]);
string_to_binary_terms([{Str, F} | Rest], Acc) ->
    Bin = list_to_binary(Str),
    FreqBin = encode_unsigned(4, F),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    FreqBin/binary,
				    <<0,0,0,0>>/binary>> | Acc]);
string_to_binary_terms([Str | Rest], Acc) when is_list(Str) ->
    Bin = list_to_binary(Str),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    <<0,0,0,0,0,0,0,0>>/binary>> | Acc]);
string_to_binary_terms([], Acc)  ->
    lists:reverse(Acc).

-spec delete_obsolete_indices(Res :: ok | {error, Reason :: term()},
			      TD :: #{},
			      IndexOn :: [{string(), term()}]) ->
    ok | {error, term()}.
delete_obsolete_indices(ok, TD, IndexOn) ->
    Mapper = maps:get(column_mapper, TD),
    Cids = get_indexed_cids(Mapper, IndexOn),
    Req = {enterdb_rdb_worker, delete_indices, [Cids]},
    Dist = maps:get(distributed, TD),
    Shards = maps:get(shards, TD),
    map_shards(Dist, Req, write, Shards);
delete_obsolete_indices(Err, _TD, _IndexOn) ->
    ?debug("Not performing delete on term indices due to: ~p", [Err]),
    ok.
