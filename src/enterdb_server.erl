%%%===================================================================
%% @author Erdem Aksu
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
%% -------------------------------------------------------------------
%% @doc
%% EnterDB Server that manages application configuration, wrapping tables
%% and possible other operations required to be done sequentially.
%% @end
%%%===================================================================

-module(enterdb_server).

-revision('$Revision: $ ').
-modified('$Date: $ ').

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([get_state_params/0,
	 wrap_level/4,
	 get_db_path/0]).

-define(SERVER, ?MODULE). 

-include("enterdb.hrl").
-include("gb_log.hrl").

-record(state, {db_path,
                num_of_local_shards}).

%% This record is stored in ets table wrapper_registry
-record(wrapper_registry, {name :: string(),
			   wrapped_ts :: timestamp(),
			   wrapped_level :: string()}).


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

-spec get_db_path() -> string().
get_db_path() ->
    CONF_PATH = gb_conf:get_param("enterdb.yaml", db_path),
    _DB_PATH =
	case CONF_PATH of
	    [$/|_] ->
		CONF_PATH;
	    _ ->
		filename:join(gb_conf_env:proddir(), CONF_PATH)
	end.

%%--------------------------------------------------------------------
%% @doc
%% Wrapping database workers invokes this function to delete and
%% recreate oldest level. This function ensures the level is
%% recreated only once for given time period.
%% @end
%%--------------------------------------------------------------------
-spec wrap_level(Mod :: atom(),
		 Name :: string(),
		 Key :: key(),
		 TimeMargin :: pos_integer()) -> ok.
wrap_level(Mod, Name, Key, TimeMargin)->
    gen_server:cast(enterdb_server,
		    {wrap_level, Mod, Name, Key, TimeMargin}).

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
    NumOfShards = case gb_conf:get_param("enterdb.yaml", num_of_local_shards) of
                      undefined ->
			  ?debug("num_of_local_shards not configured!", []),
                          erlang:system_info(schedulers);
                      Int when is_integer(Int) ->
			Int;
		      IntStr ->
                        case catch list_to_integer(IntStr) of
                            Int when is_integer(Int) ->
                                Int;
                            _ ->
                                erlang:system_info(schedulers)
                        end
                  end,
    DB_PATH = get_db_path(),
    ok = filelib:ensure_dir(DB_PATH),
    ?debug("DB_PATH: ~p", [DB_PATH]),
    ets:new(wrapper_registry, [protected, named_table, {keypos, 2}]),
    case mnesia:wait_for_tables([enterdb_table, enterdb_stab], 20000) of
            {timeout,   RemainingTabs} ->
              {stop, {no_exists,RemainingTabs}};
            ok ->
		ok = open_shards(),
		{ok, #state{db_path = DB_PATH,
			    num_of_local_shards = NumOfShards}}
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
handle_cast({wrap_level, Mod, Name, Key, TimeMargin}, State) ->
    case lists:keyfind(ts, 1, Key) of
	{ts, {Macs, Secs, Mics}} ->
	    Ts = {Macs, Secs+TimeMargin, Mics},
	    UpperLevelKey = lists:keyreplace(ts, 1, Key, {ts, Ts}),
	    {ok, {level, Level}} = gb_hash:find_node(Name, UpperLevelKey),
	    Wrap =
		case ets:lookup(wrapper_registry, Name) of
		    [#wrapper_registry{wrapped_ts = Wrapped_Ts,
			               wrapped_level = Level}] ->
			TDiff = timer:now_diff(Ts, Wrapped_Ts) / 1000000,
			(TDiff > TimeMargin);
		    _ ->
			true
		end,
	    case Wrap of
		true ->
		    Rec = #wrapper_registry{name = Name,
					    wrapped_ts = Ts,
		                            wrapped_level = Level},
		    ets:insert(wrapper_registry, Rec),
		    {ok, Shards} = gb_hash:get_nodes(Level),
		    ?debug("Wrapping level: ~p.", [Level]),
		    [spawn(Mod, recreate_shard, [Shard]) || Shard <- Shards];
		false ->
		    ok
	    end;
	{ts, _ELSE} ->
	    ?debug("Error: wrap_level with invalid timestamp: ~p", [_ELSE]);
	false ->
	    ?debug("Error: wrap_level with key without timestamp", [])
    end,
    {noreply, State};
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
   ?debug("enterdb shutting down.", []),
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
%% Open existing database table shards.
%% @end
%%--------------------------------------------------------------------
-spec open_shards() -> ok | {error, Reason :: term()}.
open_shards() ->
    case enterdb_db:transaction(fun() -> mnesia:all_keys(enterdb_stab) end) of
	{atomic, DBList} ->
	    enterdb_lib:open_shards(DBList);
	{error, Reason} ->
	    {error, Reason}
    end.
