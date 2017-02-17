%%%===================================================================
%% @author Erdem Aksu
%%--------------------------------------------------------------------
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
%%-------------------------------------------------------------------
%% @doc
%%
%% @end
%% Created : 2017-02-16 15:19:00
%%-------------------------------------------------------------------
-module(enterdb_pts).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 new/2,
	 overwrite/2,
	 lookup/2,
	 insert/3,
	 delete/2,
	 update_counter/5,
	 foldl/3,
	 foldr/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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

new(Pid, T) when is_tuple(T) ->
    gen_server:call(?MODULE, {new, Pid, T}).

overwrite(Pid, T) when is_tuple(T) ->
    Fun = fun(E, {Pos, Acc}) -> {Pos + 1, [{Pos, E} | Acc]} end,
    [_ | L] = erlang:tuple_to_list(T),
    {_, List} = lists:foldl(Fun, {2, []}, L),
    InitList = [{1, Pid} | List],
    ets:insert(enterdb_pts, erlang:make_tuple(size(T), undefined, InitList)).

insert(Pid, Pos, Value) ->
    ets:update_element(enterdb_pts, Pid, {Pos, Value}).

delete(Pid, Pos) ->
    ets:update_element(enterdb_pts, Pid, {Pos, undefined}).

lookup(Pid, Pos) ->
    ets:lookup_element(enterdb_pts, Pid, Pos).

update_counter(Pid, Pos, Incr, Threshold, SetValue) ->
    ets:update_counter(enterdb_pts, Pid, {Pos, Incr, Threshold, SetValue}).

foldl(Fun, Acc, Pid) ->
    case ets:lookup(enterdb_pts, Pid) of
	[Tuple] ->
	    [_ | Rest] = tuple_to_list(Tuple),
	    lists:foldl(Fun, Acc, Rest);
	[] ->
	    Acc
    end.

foldr(Fun, Acc, Pid) ->
    case ets:lookup(enterdb_pts, Pid) of
	[Tuple] ->
	    [_ | Rest] = tuple_to_list(Tuple),
	    lists:foldr(Fun, Acc, Rest);
	[] ->
	    Acc
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, Table} |
%%                     {ok, Table, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ?info("Starting EnterDB Public Term Storage."),
    Options = [named_table, public,
	       {read_concurrency, true},
	       {keypos, 1}],
    Table = ets:new(enterdb_pts, Options),
    {ok, Table}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, Table) ->
%%                                   {reply, Reply, Table} |
%%                                   {reply, Reply, Table, Timeout} |
%%                                   {noreply, Table} |
%%                                   {noreply, Table, Timeout} |
%%                                   {stop, Reason, Reply, Table} |
%%                                   {stop, Reason, Table}
%% @end
%%--------------------------------------------------------------------
handle_call({new, Pid, Tuple}, _From, Table) ->
    overwrite(Pid, Tuple),
    erlang:monitor(process, Pid),
    {reply, ok, Table}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, Table) -> {noreply, Table} |
%%                                  {noreply, Table, Timeout} |
%%                                  {stop, Reason, Table}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, Table) ->
    {noreply, Table}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, Table) -> {noreply, Table} |
%%                                   {noreply, Table, Timeout} |
%%                                   {stop, Reason, Table}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Pid, _Info}, Table) ->
    ets:delete(Table, Pid),
    {noreply, Table};
handle_info(_Info, Table) ->
    {noreply, Table}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, Table) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _Table) ->
    ?info("shutting down"),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, Table, Extra) -> {ok, NewTable}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, Table, _Extra) ->
    {ok, Table}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
