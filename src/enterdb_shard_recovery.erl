%%%===================================================================
%% @author Jonas Falkevik
%% @copyright 2017 Pundun Labs AB
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
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(enterdb_shard_recovery).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([log_event/2,
	 log_event/3,
	 log_event/4,
	 log_event_recover/2,
	 start/1,
	 start/2,
	 get_log/1,
	 stop/1,
	 mem_log/2,
	 dump_mem_log/1]).

-include_lib("gb_log/include/gb_log.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start(Name) ->
    supervisor:start_child(enterdb_shard_recovery_sup, [Name]).

start(Name, Type) ->
    supervisor:start_child(enterdb_shard_recovery_sup, [Name, Type]).

cast_do(Pid, Op) when is_pid(Pid) ->
    gen_server:cast(Pid, Op);
cast_do(Name, Op) ->
    case enterdb_ns:get(Name) of
	Pid when is_pid(Pid) ->
	    cast_do(Pid, Op);
	_ ->
	    {error, not_running}
    end.
call_do(Pid, Op) when is_pid(Pid) ->
    gen_server:call(Pid, Op);
call_do(Name, Op) ->
    case enterdb_ns:get(Name) of
	Pid when is_pid(Pid) ->
	    call_do(Pid, Op);
	_ ->
	    {error, not_running}
    end.

log_event(Id, Event) ->
    cast_do(Id, {log_event, Event}).

log_event(Name, Event, start) ->
    case log_event(Name, Event) of
	{error, not_running} ->
	    {ok, Pid} = start(Name),
	    log_event(Pid, Event);
	R ->
	    R
    end.

log_event(Id, Event, start, recovering) ->
    cast_do(Id, {mem_log, Event});

log_event(Id, Event, start, not_ready) ->
    cast_do(Id, {log_event, Event}).

log_event_recover(Id, Event) ->
    log_event(Id, Event, start, recovering).

get_log(Id) ->
    call_do(Id, get_log).

stop(Id) ->
    cast_do(Id, stop).

mem_log(Id, Event) ->
    cast_do(Id, {mem_log, Event}).

dump_mem_log(Id) ->
    call_do(Id, dump_mem_log).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, [Args], []).

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
init([{Node, Shard} = Name]) ->
    enterdb_ns:register_pid(self(), Name),

    %% gossip about that log is running on this node for remote shard
    gb_dyno_metadata:node_add_prop(Node, {{Shard, oos}, {log, node()}}),

    Path = filename:join([enterdb_lib:get_path(hh_path), atom_to_list(Node), Shard ++ ".log"]),
    ok = filelib:ensure_dir(Path),
    SizeMb = gb_conf:get_param("enterdb.yaml", hh_log_size, 100),
    {ok, Log} =
	disk_log:open([{name,Name},
		       {file, Path},
		       {size, 1024*1024*SizeMb},
		       {mode, read_write}]),
    {ok, #{name => Name, node => Node, shard => Shard,
	   path => Path, log => Log}};

%% move mem_log out to a dedicated gen_server
init([{Node, Shard} = Name, mem_log]) ->
    enterdb_ns:register_pid(self(), Name),
    MemLog = ets:new(mem_log, [ordered_set]),
    {ok, #{name => Name, node=> Node, shard => Shard,
	   mem_log => MemLog, mem_seq => 0}}.

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
handle_call(dump_mem_log, _From, State) ->
    EtsTable = maps:get(mem_log, State),
    dump_mem_log_int(EtsTable),
    ets:delete_all_objects(EtsTable),
    {reply, done, State};
handle_call(get_log, _From, State) ->
    Log = maps:get(log, State),
    {reply, {ok, Log}, State}.

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
handle_cast({mem_log, Event}, #{mem_log := MemLog, mem_seq := Seq} = State) ->
    ets:insert(MemLog, {Seq, Event}),
    {noreply, State#{mem_seq => Seq + 1}};

handle_cast(stop, State) ->
    disk_log:close(maps:get(log, State)),
    file:delete(maps:get(path, State)),
    {stop, normal, State};

handle_cast({log_event, Event}, State) ->
    ?debug("logging to ~p ~p", [maps:get(name, State) , Event]),
    Res = disk_log:log(maps:get(log, State), Event),
    ?debug("disk log res ~p", [Res]),
    case Res of
	{error,{full,_}} ->
	    Node = maps:get(node, State),
	    Shard = maps:get(shard, State),
	    %% gossip about full recovery needed for remote shard
	    gb_dyno_metadata:node_add_prop(Node, {{Shard, oos}, {full, node()}}),
	    disk_log:close(maps:get(log, State)),
	    file:delete(maps:get(path, State)),
	    {stop, normal, State};
	_ ->
	{noreply, State}
    end.

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
dump_mem_log_int(Ets) ->
    case ets:select(Ets, [{'_',[],['$_']}], 10000) of
	{Data, Cont} ->
	    dump_mem_log_int(Data, Cont);
	_ ->
	    ok
    end.

dump_mem_log_int(Data, Cont) when Cont /= '$end_of_table' ->
    apply_data(Data),
    case ets:select(Cont) of
	{NewData, NewCont} ->
	    dump_mem_log_int(NewData, NewCont);
	_ ->
	    ok
    end.

apply_data([{M,F,A} | R]) ->
    apply(M,F,A),
    apply_data(R);
apply_data([]) ->
    ok.
