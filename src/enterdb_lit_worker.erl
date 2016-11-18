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

-module(enterdb_lit_worker).

-behaviour(gen_server).

%% API functions
-export([start_link/1, stop/1]).

-export([first/1,
	 last/1,
	 seek/2,
	 prev/1,
	 next/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-record(state, {name,
		data_model,
		key,
		column_mapper,
		dir,
		last_key,
		iterators,
		distributed,
		monitors}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a temprorary server that manages an enterdb iterator.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args :: [{atom(), term()} | atom()]) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server. Called by an enterdb_ldb_worker process when it
%% resource should be deleted.
%% @end
%%--------------------------------------------------------------------
-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
    gen_server:cast(Pid, stop).

%%--------------------------------------------------------------------
%% @doc
%% Return the first record in table defined by Name.
%% @end
%%--------------------------------------------------------------------
-spec first(Name :: string()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
first(Name) ->
    first_(get_args(Name)).

-spec first_(ArgsT :: {ok, [{atom(), term()}]} | {error, Reason :: term()}) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
first_({ok, Args}) ->
    case supervisor:start_child(enterdb_lit_sup, [Args]) of
        {ok, Pid} ->
	    case gen_server:call(Pid, first) of
		{ok, First} ->
		    {ok, First, Pid};
		{error, invalid} ->
		    {error, invalid}
	    end;
        {error, Reason} ->
            {error, Reason}
    end;
first_({error, Reason}) ->
    {error, Reason}.

%%--------------------------------------------------------------------
%% @doc
%% Return the last record in table defined by Name.
%% @end
%%--------------------------------------------------------------------
-spec last(Name :: string()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
last(Name) ->
    last_(get_args(Name)).

-spec last_(ArgsT :: {ok, [{atom(), term()}]} | {error, Reason :: term()}) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
last_({ok, Args}) ->
    case supervisor:start_child(enterdb_lit_sup, [Args]) of
        {ok, Pid} ->
	    case gen_server:call(Pid, last) of
		{ok, Last} ->
		    {ok, Last, Pid};
		{error, invalid} ->
		    {error, invalid}
	    end;
        {error, Reason} ->
            {error, Reason}
    end;
last_({error, Reason}) ->
    {error, Reason}.

%%--------------------------------------------------------------------
%% @doc
%% Seek a record Return in table with given Key and retrun the record.
%% @end
%%--------------------------------------------------------------------
-spec seek(Name :: string(), Key :: key()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
seek(Name, Key) ->
    seek_(get_args(Name), Key).

-spec seek_(ArgsT :: {ok, [{atom(), term()}]} | {error, Reason :: term()},
	    Key :: key()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
seek_({ok, Args}, Key) ->
    case supervisor:start_child(enterdb_lit_sup, [Args]) of
        {ok, Pid} ->
	    case gen_server:call(Pid, {seek, Key}) of
		{ok, Rec} ->
		    {ok, Rec, Pid};
		{error, Reason} ->
		    {error, Reason}
	    end;
        {error, Reason} ->
            {error, Reason}
    end;
seek_({error, Reason}, _) ->
    {error, Reason}.

%%--------------------------------------------------------------------
%% @doc
%% Return the next record for iterator defined by Pid.
%% @end
%%--------------------------------------------------------------------
-spec next(Pid :: pid()) ->
    {ok, kvp()} | {error, Reason :: term()}.
next(Pid) ->
    gen_server:call(Pid, next).

%%--------------------------------------------------------------------
%% @doc
%% Return the next record for iterator defined by Pid.
%% @end
%%--------------------------------------------------------------------
-spec prev(Pid :: pid()) ->
    {ok, kvp()} | {error, Reason :: term()}.
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
init(Args) ->
    Name = proplists:get_value(name, Args),
    DataModel = proplists:get_value(data_model, Args),
    Key = proplists:get_value(key, Args),
    Mapper = proplists:get_value(column_mapper, Args),
    Comp = proplists:get_value(comparator, Args),
    Dir = enterdb_lib:comparator_to_dir(Comp),
    case gb_hash:is_distributed(Name) of
	undefined ->
	    {stop, "no_table"};
	Dist ->
	    {ok, Shards} = gb_hash:get_nodes(Name),
	    process_flag(trap_exit, true),
	    InitResult = init_iterators(Shards, Dist),
	    {ok, Iterators, Monitors} = monitor_iterators(InitResult),
	    ?debug("Started Iterators: ~p", [Iterators]),
	    State = #state{name = Name,
			   data_model = DataModel,
			   key = Key,
			   column_mapper = Mapper,
			   dir = Dir,
			   iterators = Iterators,
			   distributed = Dist,
			   monitors = Monitors},
	    {ok, State, 100}
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
handle_call(first, _From, State = #state{iterators = Iterators,
					 data_model = DataModel,
					 key = Key,
					 column_mapper = Mapper,
					 dir = Dir}) ->
    KVL_Map = iterate(Iterators, first),
    ?debug("KVL_Map: ~p", [KVL_Map]),
    FirstBin = apply_first(Dir, KVL_Map),
    CurrentKey = get_current_key(FirstBin),
    First = make_app_kvp(FirstBin, DataModel, Key, Mapper),
    {reply, First, State#state{last_key = CurrentKey}, ?ITERATOR_TIMEOUT};
handle_call(last, _From, State = #state{iterators = Iterators,
					data_model = DataModel,
					key = Key,
					column_mapper = Mapper,
					dir = Dir}) ->
    KVL_Map = iterate(Iterators, last),
    LastBin = apply_last(Dir, KVL_Map),
    CurrentKey = get_current_key(LastBin),
    Last = make_app_kvp(LastBin, DataModel, Key, Mapper),
    {reply, Last, State#state{last_key = CurrentKey}, ?ITERATOR_TIMEOUT};
handle_call({seek, SKey}, _From, State = #state{iterators = Iterators,
						data_model = DataModel,
						key = KeyDef,
						column_mapper = Mapper,
						dir = Dir}) ->
    case make_db_key(KeyDef, SKey) of
	{ok, DBKey} ->
	    KVL_Map = iterate(Iterators, {seek, DBKey}),
	    FirstBin = apply_first(Dir, KVL_Map),
	    CurrentKey = get_current_key(FirstBin),
	    First = make_app_kvp(FirstBin, DataModel, KeyDef, Mapper),
	    {reply, First, State#state{last_key = CurrentKey}, ?ITERATOR_TIMEOUT};
	{error, Reason} ->
	    {reply, {error, Reason}, State, 0}
    end;
handle_call(next, _From, State = #state{iterators = Iterators,
					last_key = LastKey,
					data_model = DataModel,
					key = Key,
					column_mapper = Mapper,
					dir = Dir}) ->
    KVL_Map = iterate(Iterators, {seek, LastKey}),
    ?debug("KVL_Map: ~p~nLastKey: ~p", [KVL_Map, LastKey]),
    NextBin = apply_next(Dir, KVL_Map, LastKey),
    CurrentKey = get_current_key(NextBin),
    Next = make_app_kvp(NextBin, DataModel, Key, Mapper),
    {reply, Next, State#state{last_key = CurrentKey}, ?ITERATOR_TIMEOUT};
handle_call(prev, _From, State = #state{iterators = Iterators,
					last_key = LastKey,
					data_model = DataModel,
					key = Key,
					column_mapper = Mapper,
					dir = Dir}) ->
    PrevBin = apply_prev(Dir, Iterators, LastKey),
    CurrentKey = get_current_key(PrevBin),
    Prev = make_app_kvp(PrevBin, DataModel, Key, Mapper),
    {reply, Prev, State#state{last_key = CurrentKey}, ?ITERATOR_TIMEOUT};
handle_call(_Request, _From, State) ->
    Reply = ?debug("Unhandled request: ~p",[_Request]),
    {reply, Reply, State, 0}.
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
handle_cast(stop, State) ->
    ?debug("Received stop msg, stoping ..",[]),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State, 0}.

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
handle_info({'DOWN', Mref, process, It, Inf},
	    #state{monitors = Monitors} = State) ->
    case maps:get(Mref, Monitors, undefined) of
	It ->
	    ?debug("Received DOWN from Iterator ~p: ~p. Stopping..", [It, Inf]),
	    {stop, normal, State};
	undefined ->
	    {noreply, State}
    end;
handle_info(timeout, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State, 0}.

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
    ?debug("Terminating ~p, Reason: ~p", [?MODULE, Reason]),
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
-spec get_args(Name :: string()) ->
    {ok, Args :: [{atom(), term()}]}.
get_args(Name) ->
    enterdb:table_info(Name,[name, data_model, key, column_mapper, comparator]).

-spec init_iterators(Shards :: shards(), Dist :: boolean()) ->
    [{ok, pid()}] | {error, Reason :: term()}.
init_iterators(Shards, Dist) ->
    Req = {enterdb_lit_resource, init_iterator, [self()]},
    enterdb_lib:map_shards(Dist, Req, Shards).

-spec iterate(Iterators :: [it()],
	      Op :: first | last | {seek, Key :: key()}) ->
    map().
iterate(Iterators, Op) ->
    Applied = [ apply_op(It, Op) || It <- Iterators],
    ?debug("Iterate ~p: ~p",[Op, Applied]),
    maps:from_list([ {KVP, It} || {ok, KVP, It} <- Applied]).

-spec apply_op(It :: it(),
	       Op :: first | last | {seek, Key :: key()}) ->
    {KVP :: kvp() | invalid, It :: it()}.
apply_op(It, first) ->
    case enterdb_lit_resource:first(It) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end;
apply_op(It, last) ->
    case enterdb_lit_resource:last(It) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end;
apply_op(It, {seek, Key}) ->
    case enterdb_lit_resource:seek(It, Key) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end.

-spec apply_first(Dir :: 0 | 1, KVL_Map :: map()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_first(Dir, KVL_Map)->
    KVL = maps:keys(KVL_Map),
    case leveldb_utils:sort_kvl(Dir, KVL) of
	{ok, []} ->
	    {error, invalid};
	{ok, [First | _]} ->
	    {ok, First};
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(~p, ~p) -> {error, ~p}",
		[Dir, KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_last(Dir :: 0 | 1, KVL_Map :: map()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_last(Dir, KVL_Map)->
    KVL = maps:keys(KVL_Map),
    case leveldb_utils:sort_kvl(opposite(Dir), KVL) of
	{ok, []} ->
	    {error, invalid};
	{ok, [Last | _]} ->
	    {ok, Last};
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(~p, ~p) -> {error, ~p}",
		[opposite(Dir), KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_next(Dir :: 0 | 1, KVL_Map :: map(), LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(Dir, KVL_Map, LastKey)->
    KVL = maps:keys(KVL_Map),
    apply_next(Dir, KVL_Map, KVL, LastKey).

-spec apply_next(Dir :: 0 | 1, KVL_Map :: map(),
		 KVL :: [kvp()], LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(_Dir, _KVL_Map, [], _LastKey) ->
    {error, invalid};
apply_next(Dir, KVL_Map, KVL, LastKey) ->
    case leveldb_utils:sort_kvl(Dir, KVL) of
	{ok, [Head | Rest]} ->
	    apply_next(Dir, KVL_Map, Head, Rest, LastKey);
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(~p, ~p) -> {error, ~p}",
		[Dir, KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_next(Dir :: 0 | 1, KVL_Map :: map(), Head :: kvp(),
		 Rest :: [kvp()], LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(Dir, KVL_Map, {LastKey, _} = Head, Rest, LastKey) ->
    LastIt = maps:get(Head, KVL_Map),
    case enterdb_lit_resource:next(LastIt) of
	{ok, KVP} ->
	    case leveldb_utils:sort_kvl(Dir, [KVP | Rest]) of
		{ok, [H|_]} -> {ok, H};
		{error, _Reason} -> {error, invalid}
	    end;
	{error, invalid} ->
	    kvl_head(Rest)
    end;
apply_next(_Dir, _KVL_Map, Head, _Rest, _LastKey) ->
    {ok, Head}.

-spec apply_prev(Dir :: 0 | 1,
		 Iterators :: [it()],
		 LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_prev(Dir, Iterators, LastKey)->
    Applied = [ apply_op(It, {seek, LastKey}) || It <- Iterators],
    ValidIterators = [ NIt || {ok, _, NIt} <- Applied],
    InvalidIterators = [ NIt || {invalid, NIt} <- Applied],
    apply_prev_last(Dir, ValidIterators,InvalidIterators).

-spec apply_prev_last(Dir :: 0 | 1,
		      ValidIterators :: [it()],
		      InvalidIterators :: [it()]) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_prev_last(_Dir, [], _)->
    {error, invalid};
apply_prev_last(Dir, ValidIterators, InvalidIterators)->
    KVL =
	lists:foldl(fun(It, Acc) ->
			case enterdb_lit_resource:prev(It) of
			    {ok, KVP} ->
				[KVP | Acc];
			    {error, invalid} ->
				Acc
			end	
		    end, [], ValidIterators),
    LastKVPs = maps:keys(iterate(InvalidIterators, last)),
    case leveldb_utils:sort_kvl(opposite(Dir), KVL++LastKVPs) of
        {ok, [H|_]} -> {ok, H};
	{ok, []} -> {error, invalid};
	{error, _Reason} -> {error, invalid}
    end.

-spec kvl_head(KVL :: [kvp()]) ->
    {ok, H :: kvp()} | {error, invalid}.
kvl_head([H|_]) ->
    {ok, H};
kvl_head(_)->
    {error, invalid}.

-spec get_current_key(Any :: {ok, kvp()} | {error, Reason :: term()}) ->
    Key :: key() | invalid.
get_current_key({error, _}) ->
    invalid;
get_current_key({_,{Key,_}}) ->
    Key.

-spec make_db_key(KeyDef :: [string()],
		  Key :: [{string(), term()}]) ->
    {ok, DbKey :: binary()} | {error, Reason :: term()}.
make_db_key(KeyDef, Key) ->
    enterdb_lib:make_db_key(KeyDef, Key).

make_app_kvp({ok, {BK, BV}}, DataModel, Key, Mapper) ->
    ?debug("make_app_key(~p,~p,~p,~p)",[{ok, BK, BV}, DataModel, Key, Mapper]),
    {ok,
     {enterdb_lib:make_app_key(Key, BK),
      enterdb_lib:make_app_value(DataModel, Mapper, BV)}};
make_app_kvp(Else, _, _, _) ->
    ?debug("make_app_key(~p,....)",[Else]),
    Else.

-spec opposite(Dir :: 0 | 1) ->
    1 | 0.
opposite(0) ->
    1;
opposite(1) ->
    0.

-spec monitor_iterators(InitResult :: [{ok, pid()}]) ->
    {ok, Iterators :: [pid()], Monitors :: map()}.
monitor_iterators(InitResult)->
    monitor_iterators(InitResult, [], maps:new()).

-spec monitor_iterators(InitResult :: [{ok, pid()}],
			AccI :: [pid()],
			AccM :: map()) ->
    {ok, Iterators :: [pid()], Monitors :: map()}.
monitor_iterators([{ok, Pid} | Rest], AccI, AccM) ->
    Mref = erlang:monitor(process, Pid),
    monitor_iterators(Rest, [Pid | AccI], maps:put(Mref, Pid, AccM));
monitor_iterators([], AccI, AccM) ->
    {ok, AccI, AccM}.
