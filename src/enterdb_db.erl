%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%%
%%% @end
%%% Created :  15 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_db).

%% API
-export([create_tables/1]).

-export([transaction/1]).

-include("enterdb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create tables on given Nodes.
%% @end
%%--------------------------------------------------------------------
-spec create_tables(Nodes :: [node()]) -> ok | {error, Reason :: any()}.
create_tables(Nodes) ->
    [create_table(Nodes, T) || T <- [enterdb_stab,
				     enterdb_table,
				     enterdb_ldb_resource,
				     enterdb_it_resource]].

%%--------------------------------------------------------------------
%% @doc
%% Run mnesia activity with access context transaction with given fun
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: fun()) ->
    {aborted, Reason :: term()} | {atomic, ResultOfFun :: term()}.
transaction(Fun) ->
    case catch mnesia:activity(transaction, Fun) of
        {'EXIT', Reason} ->
            {error, Reason};
        Result ->
            {atomic, Result}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec create_table(Nodes::[node()], Name::atom()) -> ok | {error, Reason::term()}.
create_table(Nodes, Name) when Name == enterdb_stab ->
    TabDef = [{access_mode, read_write},
              {attributes, record_info(fields, enterdb_stab)},
              {disc_copies, Nodes},
              {local_content, true}, %% data is local only to this node
	      {load_order, 49},
              {record_name, Name},
              {type, set}
             ],
    mnesia:create_table(Name, TabDef);
create_table(Nodes, Name) when Name == enterdb_table->
    TabDef = [{access_mode, read_write},
              {attributes, record_info(fields, enterdb_table)},
              {disc_copies, Nodes},
              {load_order, 49},
              {record_name, Name},
              {type, set}
             ],
    mnesia:create_table(Name, TabDef);
create_table(Nodes, Name) when Name == enterdb_ldb_resource->
    TabDef = [{access_mode, read_write},
              {attributes, record_info(fields, enterdb_ldb_resource)},
              {ram_copies, Nodes},
              {load_order, 49},
              {record_name, Name},
              {type, set}
             ],
    mnesia:create_table(Name, TabDef);
create_table(_, _) ->
    {error, "Unknown table definition"}.
