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
         get_shards/2,
         create_leveldb_db/2]).

-include("enterdb.hrl").
%%%===================================================================
%%% API
%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Verify the args given to enterdb:create_table/5
%%
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
%%  has unique elements
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
verify_table_options([Elem|_])->
    {error, {Elem, "unknown_option"}}.

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
%% Create and return #enterdb_shard{} records for local sharding.
%%
%%--------------------------------------------------------------------
-spec get_shards(Name::string(), NumOfShards::pos_integer()) -> {ok, Shards::[#enterdb_shard{}]}.
get_shards(Name, NumOfShards) ->
    ShardNames = [lists:concat([Name,"_shard_",N]) ||N <- lists:seq(1, NumOfShards)],
    Shards = [#enterdb_shard{hash = gb_chash:chash(SN), name = SN} || SN <- ShardNames],
    {ok, lists:keysort(#enterdb_shard.hash, Shards)}.

%%--------------------------------------------------------------------
%% @doc
%% Create leveldb dbs on DB_PATH, specified by Shards.
%%
%%--------------------------------------------------------------------
-spec create_leveldb_db(DB_PATH::string(),
                        Shards::[#enterdb_shard{}]) -> ok |
                                                       {error, Reason::term()}.
create_leveldb_db(DB_PATH, Shards) ->
    create_leveldb_db([{create_if_missing, true},
                       {error_if_exists, true}], DB_PATH, Shards).

create_leveldb_db(_LDB_Options, _DB_PATH, []) ->
    ok;
create_leveldb_db(LDB_Options, DB_PATH, [#enterdb_shard{name = Name,
                                                        hash = Hash}|Rest]) ->
    ChildArgs = [{name, Name}, {hash, Hash},
                 {options, LDB_Options}, {path, DB_PATH}],
    case supervisor:start_child(enterdb_ldb_sup, [ChildArgs]) of
        {ok, _Pid} ->
            create_leveldb_db(LDB_Options, DB_PATH, Rest);
        {error, Reason} ->
            {error, Reason}
    end.

