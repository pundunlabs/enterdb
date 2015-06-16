-module(enterdb_lit_worker).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

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
-include("gb_log.hrl").

-record(state, {name,
		data_model,
		key,
		columns,
		last_key,
		iterators}).

-define(TIMEOUT, 10000).

-define(ASCENDING, 1).
-define(DESCENDING, 0).
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
%% Return the first record in table defined by Name.
%% @end
%%--------------------------------------------------------------------
-spec first(Name :: string()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
first(Name) ->
    Args = enterdb:table_info(Name,[name,data_model,key,columns]),
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
    end.

%%--------------------------------------------------------------------
%% @doc
%% Return the last record in table defined by Name.
%% @end
%%--------------------------------------------------------------------
-spec last(Name :: string()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
last(Name) ->
    Args = enterdb:table_info(Name,[name,data_model,key,columns]),
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
    end.

%%--------------------------------------------------------------------
%% @doc
%% Seek a record Return in table with given Key and retrun the record.
%% @end
%%--------------------------------------------------------------------
-spec seek(Name :: string(), Key :: key()) ->
    {ok, kvp(), pid()} | {error, Reason :: term()}.
seek(Name, Key) ->
    Args = enterdb:table_info(Name,[name,data_model,key,columns]),
    case supervisor:start_child(enterdb_lit_sup, [Args]) of
        {ok, Pid} ->
	    case gen_server:call(Pid, {seek, Key}) of
		{ok, Rec} ->
		    {ok, Rec, Pid};
		{error, invalid} ->
		    {error, invalid}
	    end;
        {error, Reason} ->
            {error, Reason}
    end.

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
    Columns = proplists:get_value(columns, Args),
    case gb_hash:get_nodes(Name) of
	undefined ->
	    {stop, "no_table"};
	{ok, Shards} ->
	    Iterators = init_iterators(Shards),
	    State = #state{name = Name,
			   data_model = DataModel,
			   key = Key,
			   columns = Columns,
			   iterators = Iterators},
	    %register_it(State),
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
					 columns = Columns}) ->
    KVL_Map = iterate(Iterators, first),
    ?debug("KVL_Map: ~p", [KVL_Map]),
    FirstBin = apply_first(KVL_Map),
    CurrentKey = get_current_key(FirstBin),
    First = make_app_kvp(FirstBin, DataModel, Key, Columns),
    {reply, First, State#state{last_key = CurrentKey}, ?TIMEOUT};
handle_call(last, _From, State = #state{iterators = Iterators,
					data_model = DataModel,
					key = Key,
					columns = Columns}) ->
    KVL_Map = iterate(Iterators, last),
    LastBin = apply_last(KVL_Map),
    CurrentKey = get_current_key(LastBin),
    Last = make_app_kvp(LastBin, DataModel, Key, Columns),
    {reply, Last, State#state{last_key = CurrentKey}, ?TIMEOUT};
handle_call({seek, SKey}, _From, State = #state{iterators = Iterators,
						data_model = DataModel,
						key = KeyDef,
						columns = Columns}) ->
    {ok, DBKey} = make_db_key(KeyDef, SKey),
    KVL_Map = iterate(Iterators, {seek, DBKey}),
    FirstBin = apply_first(KVL_Map),
    CurrentKey = get_current_key(FirstBin),
    First = make_app_kvp(FirstBin, DataModel, KeyDef, Columns),
    {reply, First, State#state{last_key = CurrentKey}, ?TIMEOUT};
handle_call(next, _From, State = #state{iterators = Iterators,
					last_key = LastKey,
					data_model = DataModel,
					key = Key,
					columns = Columns}) ->
    KVL_Map = iterate(Iterators, {seek, LastKey}),
    ?debug("KVL_Map: ~p~nLastKey: ~p", [KVL_Map, LastKey]),
    NextBin = apply_next(KVL_Map, LastKey),
    CurrentKey = get_current_key(NextBin),
    Next = make_app_kvp(NextBin, DataModel, Key, Columns),
    {reply, Next, State#state{last_key = CurrentKey}, ?TIMEOUT};
handle_call(prev, _From, State = #state{iterators = Iterators,
					last_key = LastKey,
					data_model = DataModel,
					key = Key,
					columns = Columns}) ->
    PrevBin = apply_prev(Iterators, LastKey),
    CurrentKey = get_current_key(PrevBin),
    Prev = make_app_kvp(PrevBin, DataModel, Key, Columns),
    {reply, Prev, State#state{last_key = CurrentKey}, ?TIMEOUT};
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
terminate(_Reason, #state{iterators = Iterators}) ->
    [ leveldb:delete_iterator(It) || It <- Iterators].

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
-spec init_iterators(Shards :: [string()]) ->
    [it()] | {error, Reason :: term()}.
init_iterators(Shards) ->
    Node = node(),
    [init_iterator(Shard) || {N, Shard} <- Shards, N == Node].

-spec init_iterator(Shard :: string()) ->
    It :: it().
init_iterator(Shard) ->
    {ok, It} = enterdb_ldb_worker:get_iterator(Shard),
    It.

-spec iterate(Iterators :: [it()], Op :: first | last | {seek, Key :: key()}) ->
    map().
iterate(Iterators, Op) ->
    Applied = [ apply_op(It, Op) || It <- Iterators],
    ?debug("Iterate ~p: ~p",[Op, Applied]),
    maps:from_list([ {KVP, It} || {ok, KVP, It} <- Applied]).

-spec apply_op(It :: it(), Op :: first | last | {seek, Key :: key()}) ->
    {KVP :: kvp() | invalid, It :: it()}.
apply_op(It, first) ->
    case leveldb:first(It) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end;
apply_op(It, last) ->
    case leveldb:last(It) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end;
apply_op(It, {seek, Key}) ->
    case leveldb:seek(It, Key) of
	{ok, KVP} ->
	    {ok, KVP, It};
	{error, _} ->
	    {invalid, It}
    end.

-spec apply_first(KVL_Map :: map()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_first(KVL_Map)->
    KVL = maps:keys(KVL_Map),
    case leveldb_utils:sort_kvl(?DESCENDING, KVL) of
	{ok, []} ->
	    {error, invalid};
	{ok, [First | _]} ->
	    {ok, First};
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(?DESCENDING, ~p) -> {error, ~p}",[KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_last(KVL_Map :: map()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_last(KVL_Map)->
    KVL = maps:keys(KVL_Map),
    case leveldb_utils:sort_kvl(?ASCENDING, KVL) of
	{ok, []} ->
	    {error, invalid};
	{ok, [Last | _]} ->
	    {ok, Last};
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(?ASCENDING, ~p) -> {error, ~p}",[KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_next(KVL_Map :: map(), LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(KVL_Map, LastKey)->
    KVL = maps:keys(KVL_Map),
    apply_next(KVL_Map, KVL, LastKey).

-spec apply_next(KVL_Map :: map(), KVL :: [kvp()], LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(_KVL_Map, [], _LastKey) ->
    {error, invalid};
apply_next(KVL_Map, KVL, LastKey) ->
    case leveldb_utils:sort_kvl(?DESCENDING, KVL) of
	{ok, [Head | Rest]} ->
	    apply_next(KVL_Map, Head, Rest, LastKey);
	{error, Reason} ->
	    ?debug("leveldb_utils:sort_kvl(?DESCENDING, ~p) -> {error, ~p}",[KVL, Reason]),
	    {error, invalid}
    end.

-spec apply_next(KVL_Map :: map(), Head :: kvp(),
		 Rest :: [kvp()], LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_next(KVL_Map, {LastKey, _} = Head, Rest, LastKey) ->
    LastIt = maps:get(Head, KVL_Map),
    case leveldb:next(LastIt) of
	{ok, KVP} ->
	    case leveldb_utils:sort_kvl(?DESCENDING, [KVP | Rest]) of
		{ok, [H|_]} -> {ok, H};
		{error, _Reason} -> {error, invalid}
	    end;
	{error, invalid} ->
	    kvl_head(Rest)
    end;
apply_next(_KVL_Map, Head, _Rest, _LastKey) ->
    {ok, Head}.

-spec apply_prev(Iterators :: [it()], LastKey :: key()) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_prev(Iterators, LastKey)->
    Applied = [ apply_op(It, {seek, LastKey}) || It <- Iterators],
    ValidIterators = [ It || {ok, _, It} <- Applied],
    InvalidIterators = [ It || {invalid, It} <- Applied],
    apply_prev_last(ValidIterators,InvalidIterators).

-spec apply_prev_last(ValidIterators :: [it()],
		      InvalidIterators :: [it()]) ->
    {ok, KVP :: kvp()} | {error, invalid}.
apply_prev_last([], _)->
    {error, invalid};
apply_prev_last(ValidIterators, InvalidIterators)->
    KVL =
	lists:foldl(fun(It, Acc) ->
			case leveldb:prev(It) of
			    {ok, KVP} ->
				[KVP | Acc];
			    {error, invalid} ->
				Acc
			end	
		    end, [], ValidIterators),
    LastKVPs = maps:keys(iterate(InvalidIterators, last)),
    case leveldb_utils:sort_kvl(?ASCENDING, KVL++LastKVPs) of
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

-spec make_db_key(KeyDef :: [atom()],
               Key :: [{atom(), term()}]) ->
    {ok, DbKey :: binary} | {error, Reason :: term()}.
make_db_key(KeyDef, Key) ->
    enterdb_lib:make_db_key(KeyDef, Key).

make_app_kvp({ok, {BK, BV}}, DataModel, Key, Columns) ->
    ?debug("make_app_key(~p,~p,~p,~p)",[{ok, BK, BV}, DataModel, Key, Columns]),
    {ok,
     {enterdb_lib:make_app_key(Key, BK),
      enterdb_lib:make_app_value(DataModel, Columns, BV)}};
make_app_kvp(Else, _, _, _) ->
    ?debug("make_app_key(~p,....)",[Else]),
    Else.
