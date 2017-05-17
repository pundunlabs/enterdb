%%%===================================================================
%% @author Jonas Falkevik
%%--------------------------------------------------------------------
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
%%-------------------------------------------------------------------
%% @doc
%%
%% @end
%% Created : 2015-06-17 16:10:00
%%-------------------------------------------------------------------
-module(enterdb_ns).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 get/1,
	 register_pid/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {ns_table}).
-record(ns_entry, {key, value}).

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

register_pid(Pid, Name) ->
    gen_server:call(?MODULE, {register_pid, Pid, Name}).

get(Key) ->
    case ets:lookup(enterdb_ns, Key) of
	[#ns_entry{value=Value}] ->
	    Value;
	_ ->
	    {error, no_ns_entry}
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
    ?info("Starting EnterDB Name Server."),
    Options = [named_table, public,
	       {read_concurrency, true},
	       {keypos, #ns_entry.key}],
    NsTable = ets:new(enterdb_ns, Options),
    {ok, #state{ns_table=NsTable}}.

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
handle_call({register_pid, Pid, Name}, _From, State) ->
    ets:insert(State#state.ns_table, #ns_entry{key=Name, value=Pid}),
    ets:insert(State#state.ns_table, #ns_entry{key=Pid, value=Name}),
    erlang:monitor(process, Pid),
    {reply, ok, State}.

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
    delete_pid(State#state.ns_table, Pid),
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
	[#ns_entry{value = Name}] ->
	    ets:delete(Tab, Pid),
	    ets:delete(Tab, Name);
	_ ->
	    true
    end.
