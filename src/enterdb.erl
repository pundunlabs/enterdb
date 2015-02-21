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
         read/2,
         write/3,
         delete/2]).

-include("enterdb.hrl").

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
%%--------------------------------------------------------------------
-spec create_table(Name::string(), Key::[atom()],
                   Columns::[atom()], Indexes::[atom()],
                   Options::[table_option()])-> 
    ok | {error, Reason::term()}.
create_table(Name, Key, Columns, Indexes, Options)->
    case gen_server:call(enterdb_server,{create_table, {Name, Key, 
                                                        Columns, Indexes,
                                                        Options}}) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads Key from table with name Name
%% @end
%%--------------------------------------------------------------------
-spec read(Name::string(),
           Key::[{atom(), term()}]) -> {ok, Value::term()} |
                                       {error, Reason::term()}.
read(Name, Key)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
        {ok, Shard} ->
            enterdb_ldb_worker:read(Shard, Key)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Writes Key/Columns to table with name Name
%% @end
%%--------------------------------------------------------------------
-spec write(Name::string(),
            Key::[{atom(), term()}],
            Columns::[{atom(), term()}]) -> ok | {error, Reason::term()}.
write(Name, Key, Columns)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
        {ok, Shard} ->
            enterdb_ldb_worker:write(Shard, Key, Columns)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from table with name Name
%% @end
%%--------------------------------------------------------------------
-spec delete(Name::string(),
             Key::[{atom(), term()}]) -> ok |
                                         {error, Reason::term()}.
delete(Name, Key)->
    case gb_hash:find_node(Name, Key) of
        undefined ->
            {error, "no_table"};
        {ok, Shard} ->
            enterdb_ldb_worker:delete(Shard, Key)
    end.

