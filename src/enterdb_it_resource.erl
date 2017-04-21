%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2016 Pundun Labs AB
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
%% Module Description:
%% @end
%%%===================================================================


-module(enterdb_it_resource).

-behaviour(gen_server).

%% API functions
-export([start/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Inter-Node API
-export([init_iterator/3,
	 first/1,
	 last/1,
	 seek/2,
	 next/1,
	 prev/1]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-record(state, {mod,
		it,
		mref,
		caller}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Args) ->
    gen_server:start(?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% Start a simple worker, get a level db iterator, enter
%% server loop and retrun server pid.
%% @end
%%--------------------------------------------------------------------
-spec init_iterator(Shard :: string(), Caller :: pid(), Mod :: atom()) ->
    Pid :: pid().
init_iterator(Shard, Caller, Mod) ->
    case ?MODULE:start([Shard, Caller, Mod]) of
        {ok, Pid} ->
	    {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Applies {leveldb, first, [It]} where It = State#state.it
%% @end
%%--------------------------------------------------------------------
-spec first(Pid :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: term()}.
first(Pid) ->
    gen_server:call(Pid, first).

%%--------------------------------------------------------------------
%% @doc
%% Applies {leveldb, last, [It]} where It = State#state.it
%% @end
%%--------------------------------------------------------------------
-spec last(Pid :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: term()}.
last(Pid) ->
    gen_server:call(Pid, last).

%%--------------------------------------------------------------------
%% @doc
%% Applies {leveldb, seek, [It, Key]} where It = State#state.it
%% @end
%%--------------------------------------------------------------------
-spec seek(Pid :: pid(), Key :: term()) ->
    {ok, KVP :: kvp()} | {error, Reason :: term()}.
seek(Pid, Key) ->
    gen_server:call(Pid, {seek, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Applies {leveldb, next, [It]} where It = State#state.it
%% @end
%%--------------------------------------------------------------------
-spec next(Pid :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: term()}.
next(Pid) ->
    gen_server:call(Pid, next).

%%--------------------------------------------------------------------
%% @doc
%% Applies {leveldb, prev, [It]} where It = State#state.it
%% @end
%%--------------------------------------------------------------------
-spec prev(Pid :: pid()) ->
    {ok, KVP :: kvp()} | {error, Reason :: term()}.
prev(Pid) ->
    gen_server:call(Pid, prev).

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
init([Shard, Caller, Mod]) ->
    ?debug("Init iterator from shard: ~p",[Shard]),
    process_flag(trap_exit, true),
    Mref = erlang:monitor(process, Caller),
    case enterdb_ldb_worker:get_iterator(self(), Shard) of
	{ok, It} ->
	    {ok, #state{mod = Mod,
			it = It,
			mref = Mref,
			caller = Caller}};
	Else ->
	    {stop, Else}
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
handle_call(first, _From, State = #state{mod = Mod, it = It}) ->
    Reply = Mod:first(It),
    {reply, Reply, State};
handle_call(last, _From, State = #state{mod = Mod, it = It}) ->
    Reply = Mod:last(It),
    {reply, Reply, State};
handle_call({seek, Key}, _From, State = #state{mod = Mod, it = It}) ->
    Reply = Mod:seek(It, Key),
    {reply, Reply, State};
handle_call(next, _From, State = #state{mod = Mod, it = It}) ->
    Reply = Mod:next(It),
    {reply, Reply, State};
handle_call(prev, _From, State = #state{mod = Mod, it = It}) ->
    Reply = Mod:prev(It),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    ?debug("Unhandled call request: ~p",[_Request]),
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
handle_info({'DOWN', Mref, process, Caller, _info},
	    #state{mref = Mref, caller = Caller} = State) ->
    ?debug("Received DOWN for enterdb_it_worker: ~p, stopping..", [Caller]),
    {stop, normal, State};
handle_info(timeout, State) ->
    {stop, normal, State};
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
terminate(Reason, _State) ->
    ?debug("Terminating iterator, Reason: ~p", [Reason]),
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
