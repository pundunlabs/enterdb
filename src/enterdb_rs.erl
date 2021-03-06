%%%===================================================================
%% @author Erdem Aksu
%%--------------------------------------------------------------------
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
%%-------------------------------------------------------------------
%% @doc
%%
%% @end
%% Created : 2016-11-28 16:10:00
%%-------------------------------------------------------------------
-module(enterdb_rs).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 get/1,
	 register_pid/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {rs_table,
		ref}).

-include_lib("gb_log/include/gb_log.hrl").

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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_pid(Pid :: pid()) ->
    {ok, Ref :: binary()}.
register_pid(Pid) ->
    gen_server:call(?MODULE, {register_pid, Pid}).

-spec get(Key :: term()) ->
    pid() | binary() | {error, not_found}.
get(Key) ->
    case ets:lookup(enterdb_rs, Key) of
	[{_,Value}] ->
	    Value;
	_ ->
	    {error, not_found}
    end.

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
    ?info("Starting EnterDB Reference Server."),
    Options = [named_table, public,
	       {read_concurrency, true}],
    RsTable = ets:new(enterdb_rs, Options),
    ets:insert(RsTable, {'$ref', 0}),
    {ok, #state{rs_table=RsTable}}.

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
handle_call({register_pid, Pid}, _From, State) ->
    Ref = ets:update_counter(State#state.rs_table, '$ref', {2,1,4294967295,0}),
    Bin = binary:encode_unsigned(Ref, big),
    ets:insert(State#state.rs_table, {Bin, Pid}),
    ets:insert(State#state.rs_table, {Pid, Bin}),
    erlang:monitor(process, Pid),
    {reply, {ok, Bin}, State}.

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
handle_info({'DOWN', _Ref, process, Pid, _Info}, State) ->
    delete_pid(State#state.rs_table, Pid),
    {noreply, State};
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
    ?info("shutting down"),
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

delete_pid(Tab, Pid) ->
    case ets:lookup(Tab, Pid) of
	[{_,Ref}] ->
	    ets:delete(Tab, Pid),
	    ets:delete(Tab, Ref);
	_ ->
	    true
    end.
