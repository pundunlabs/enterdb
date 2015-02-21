%%% _________________________________________________________________________
%%% Copyright (C) 2010 by
%%% MobileArts
%%% S-111 61 Stockholm
%%% Sweden
%%%
%%% Email:    info@mobilearts.se
%%% Homepage: http://www.mobilearts.se
%%%
%%% This file contains confidential information that remains the property
%%% of MobileArts, and may not be used, copied, circulated, or distributed
%%% without prior written consent of MobileArts.
%%% _________________________________________________________________________
%%%
%%% Revision:         '$Id: $'
%%% Author:           'erdem.aksu@mobilearts.com'
%%% This version:     '$Revision: $'
%%% Last modified:    '$Date: $'
%%% Last modified by: '$Author: $'
%%% _________________________________________________________________________
%%%
%%%
%%%   TODO: <Description of module>
%%%
%%% _________________________________________________________________________


-module(enterdb_server).

-revision('$Revision: $ ').
-modified('$Date: $ ').

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {db_path,
                num_of_local_shards}).

-include("enterdb.hrl").

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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

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
init([]) ->
    NumOfShards = case gb_conf:get_param("enterdb.json", num_of_local_shards, "default") of
                      "default" ->
                          erlang:system_info(schedulers);
                      IntStr ->
                        case catch list_to_integer(IntStr) of
                            Int when is_integer(Int) ->
                                Int;
                            _ ->
                                erlang:system_info(schedulers)
                        end
                  end,
    DB_PATH = gb_conf:get_param("enterdb.json", db_path),

    {ok, #state{db_path = DB_PATH,
                num_of_local_shards = NumOfShards}}.

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
handle_call({create_table,{Name, Key, 
                           Columns, Indexes,
                           Options}}, _From,
                          State = #state{db_path = DB_PATH,
                                         num_of_local_shards = NumOfShards}) ->
    Reply =
        case enterdb_lib:verify_create_table_args([{name, Name},
                                                   {key, Key},
                                                   {columns, Columns},
                                                   {indexes,Indexes},
                                                   {options, Options}]) of
            {ok, EnterdbTable} ->
                {ok, Shards} = enterdb_lib:get_shards(Name, NumOfShards),
                NewEnterdbTable = EnterdbTable#enterdb_table{path = DB_PATH,
                                                             shards = Shards},
                create_table(NewEnterdbTable),
                ok;
            {error, Reason} ->
                {error, Reason}
        end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
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
terminate(_Reason, _State) ->
    ok.

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
%% Create table according to table specification
%%
%%--------------------------------------------------------------------
-spec create_table(EnterdbTable::#enterdb_table{}) -> ok | {error, Reason::term()}. 
create_table(#enterdb_table{options = Opts} = EnterdbTable)->
    case proplists:get_value(backend, Opts) of
        leveldb ->
            case enterdb_lib:create_leveldb_db(EnterdbTable) of
                ok -> write_enterdb_table(EnterdbTable);
                {error, Reason} ->
                    error_logger:error_msg("Could not create leveldb database, error: ~p~n", [Reason]),
                    {error, Reason}
            end;
        Else ->
            error_logger:error_msg("Could not create ~p database, error: not_supported yet.~n", [Else]),
            {error, "no_supported_backend"}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Store the #enterdb_table entry in mnesia disc_copy
%%
%%--------------------------------------------------------------------
-spec write_enterdb_table(EnterdbTable::#enterdb_table{}) -> ok | {error, Reason::term()}.
write_enterdb_table(EnterdbTable) ->
    case enterdb_db:transaction(fun() -> mnesia:write(EnterdbTable) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end. 
