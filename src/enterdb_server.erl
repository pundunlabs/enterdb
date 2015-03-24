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

-export([get_state_params/0]).

-define(SERVER, ?MODULE). 

-record(state, {db_path,
                num_of_local_shards}).

-include("enterdb.hrl").
-include("gb_log.hrl").

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

-spec get_state_params() -> {ok, [{Attr :: atom(), Val :: atom()}]}.
get_state_params() ->
    gen_server:call(enterdb_server, get_state_params).

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
    ?debug("DB_PATH: ~p", [DB_PATH]),
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
handle_call(get_state_params, _From,
	    State = #state{db_path = DB_PATH,
                           num_of_local_shards = NumOfShards}) ->
    PropList = [{db_path, DB_PATH},
		{num_of_local_shards, NumOfShards}],
    {reply, {ok, PropList}, State};
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

