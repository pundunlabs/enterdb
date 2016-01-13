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
%% Module Description:
%% @end
%%%===================================================================


-module(enterdb_ldb_wrp).

-behaviour(gen_server).

%% gen_server API
-export([start_link/0]).

%% API exports
-export([create_bucket_list/2,
	 init_buckets/3,
	 read/2,
	 write/4,
	 delete/2,
	 close_shard/1,
	 delete_shard/1]).

%% Exports for self spawned process
-export([size_wrap/2,
	 time_wrap/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {bucket_table,
		counter,
		references}).

-record(entry, {key, value}).

-include("enterdb.hrl").
-include("gb_log.hrl").

-define(COUNTER_TRESHOLD, 1000).

-define(BUCKET, enterdb_wrp_buckets).
-define(COUNTER, enterdb_wrp_counter).
-define(REFERENCE, enterdb_wrp_references).

%%%===================================================================
%%% API functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid} | ignore | {error, Error :: term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Creates a bucket list.
%% @end
%%--------------------------------------------------------------------
-spec create_bucket_list(Shard :: string(),
			 Wrapper :: #enterdb_wrapper{} ) ->
    Buckets :: [string()].
create_bucket_list(Shard,
		   #enterdb_wrapper{num_of_buckets = NumOfBuckets} = Wrapper) ->
    ?debug("Creating bucket names for wrapped shard: ~p, wrapper: ~p",
	   [Shard, Wrapper]),
    [lists:concat([Shard, "_", Index]) ||
	Index <- lists:seq(0, NumOfBuckets-1)].

%%--------------------------------------------------------------------
%% @doc
%% Initiate buckets for a wrapping leveldb table shard into ets.
%% This Ets register will be used to find the bucket to be 
%% written and read.
%% @end
%%--------------------------------------------------------------------
-spec init_buckets(Shard :: shard_name(),
		   Buckets :: [shard_name()],
		   Wrapper :: #enterdb_wrapper{}) -> ok.
init_buckets(Shard, Buckets, W = #enterdb_wrapper{time_margin = TimeMargin}) ->
    ?debug("Initiating buckets for wrapped shard: ~p, wrapper: ~p", [Shard, W]),
    ets:insert(?COUNTER, #entry{key=Shard,value=0}),
    true = register_buckets(Shard, Buckets),
    register_timeout(Shard, TimeMargin).

%%--------------------------------------------------------------------
%% @doc
%% Read Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec read(Shard :: string(),
	   Key :: binary()) ->
    {ok, Value :: term()} | {error, Reason :: term()}.
read(Shard, Key) ->
    Buckets = get_buckets(Shard),
    read_from_buckets(Buckets, Key).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Columns to given shard.
%% @end
%%--------------------------------------------------------------------
-spec write(Shard :: string(),
	    Wrapper :: #enterdb_wrapper{},
            Key :: binary(),
            Columns :: binary()) -> ok | {error, Reason :: term()}.
write(Shard, #enterdb_wrapper{size_margin = SizeMargin}, Key, Columns) ->
    UpdateOp = {#entry.value, 1, ?COUNTER_TRESHOLD, 0},
    Count =  ets:update_counter(?COUNTER, Shard, UpdateOp),
    check_counter_wrap(Count, Shard, SizeMargin),
    [Bucket|_]  = get_buckets(Shard),
    enterdb_ldb_worker:write(Bucket, Key, Columns).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
	     Key :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Shard, Key) ->
    Buckets = get_buckets(Shard),
    delete_from_buckets(Buckets, Key).

%%--------------------------------------------------------------------
%% @doc
%% Close leveldb workers and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec close_shard(Shard :: shard_name()) -> ok | {error, Reason :: term()}.
close_shard(Shard) ->
    Buckets = get_buckets(Shard),
    ets:delete(?REFERENCE, Shard),
    ets:delete(?COUNTER, Shard),
    ets:delete(?BUCKET, Shard),
    Res = [supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(B)) ||
	    B <- Buckets],
    enterdb_lib:check_error_response(lists:usort(Res)).

%%--------------------------------------------------------------------
%% @doc
%% Delete leveldb buckets and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec delete_shard(Shard :: shard_name()) -> ok | {error, Reason :: term()}.
delete_shard(Shard) ->
    Buckets = get_buckets(Shard),
    [ ok = enterdb_ldb_worker:delete_db(Bucket) || Bucket <- Buckets],
    ets:delete(?REFERENCE, Shard),
    ets:delete(?COUNTER, Shard),
    ets:delete(?BUCKET, Shard).

-spec size_wrap(Shard :: shard_name(),
		SizeMargin :: size_margin()) ->
    ok.
size_wrap(Shard, SizeMargin) ->
    [Bucket|_]  = get_buckets(Shard),
    {ok, Size} =  enterdb_ldb_worker:approximate_size(Bucket),
    case size_exceeded(Size, SizeMargin) of
	true ->
	    gen_server:call(?MODULE, {wrap, Shard, Bucket});
	false ->
	    ok
    end.

-spec time_wrap(Shard :: shard_name()) ->
    ok.
time_wrap(Shard) ->
    [Bucket|_] = get_buckets(Shard),
    ok = gen_server:call(?MODULE, {wrap, Shard, Bucket}).

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
    ?info("Starting EnterDB LevelDB Wrapper Server."),
    Options = [named_table, public,
	       {read_concurrency, true},
	       {keypos, #entry.key}],
    BucketTable = ets:new(?BUCKET, Options),
    Counter = ets:new(?COUNTER, Options),
    References = ets:new(?REFERENCE, Options),
    {ok, #state{bucket_table=BucketTable,
		counter=Counter,
		references=References}}.
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
handle_call({wrap, Shard, Bucket}, _From, State) ->
    case get_buckets(Shard) of
	[Bucket | _] = Buckets ->
	    LastBucket = lists:last(Buckets),
	    ok = enterdb_ldb_worker:recreate_shard(LastBucket),
	    WrappedBuckets = [LastBucket | lists:droplast(Buckets)],
	    reset_timer(Shard),
	    true = register_buckets(Shard, WrappedBuckets);
	_ ->
	    ok
    end,
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
-spec register_buckets(Name :: shard_name(), Buckets :: [shard_name()]) ->
    true.
register_buckets(Name, Buckets) ->
    ok = enterdb_lib:update_bucket_list(Name, Buckets),
    ets:insert(?BUCKET, #entry{key=Name, value=Buckets}).

-spec get_buckets(Name :: shard_name()) ->
    ok.
get_buckets(Name) ->
    case ets:lookup(?BUCKET, Name) of
	[#entry{value=Value}] ->
	    Value;
	_ ->
	    {error, no_entry}
    end.

-spec register_timeout(Shard :: shard_name(),
		       TimeMargin :: time_margin()) -> ok.
register_timeout(_, undefined) ->
    ok;
register_timeout(Shard, TimeMargin) ->
    Time = calc_milliseconds(TimeMargin),
    {ok, Tref} = timer:apply_after(Time, ?MODULE, time_wrap, [Shard]),
    true = register_timer_ref(Shard, [{tref, Tref},{time_margin, TimeMargin}]),
    ok.

-spec register_timer_ref(Name :: shard_name(), PList :: [{atom, term()}]) ->
    true.
register_timer_ref(Name, PList) ->
    ets:insert(?REFERENCE, #entry{key=Name, value=PList}).

-spec reset_timer(Shard :: shard_name()) ->
    ok.
reset_timer(Shard) ->
    case ets:lookup(?REFERENCE, Shard) of
	[] ->
	    ok;
	[#entry{key=Name, value=PList}] ->
	    Tref = proplists:get_value(tref, PList),
	    timer:cancel(Tref),
	    TimeMargin = proplists:get_value(time_margin, PList),
	    register_timeout(Shard, TimeMargin)
    end.

-spec calc_milliseconds(TimeMargin :: time_margin()) ->
    pos_integer().
calc_milliseconds({seconds, Seconds}) ->
    Seconds * 1000;
calc_milliseconds({minutes, Minutes}) ->
    Minutes * 60000;
calc_milliseconds({hours, Hours}) ->
    Hours * 3600000.

-spec check_counter_wrap(Count :: integer(),
			 Shard :: shard_name(),
			 SizeMargin :: size_margin()) ->
    ok.
check_counter_wrap(_Count, _Shard, undefined) ->
    ok;
check_counter_wrap(Count, _Shard, _SizeMargin)
    when Count < ?COUNTER_TRESHOLD ->
    ok;
check_counter_wrap(Count, Shard, SizeMargin)
    when Count == ?COUNTER_TRESHOLD ->
    erlang:spawn(?MODULE, size_wrap, [Shard, SizeMargin]).

-spec size_exceeded(Size :: integer(),
		    SizeMargin :: size_margin()) ->
    true | false.
size_exceeded(Size, SizeMargin) ->
    BytesMargin = calc_bytes(SizeMargin),
    Size > BytesMargin.

-spec calc_bytes(SizeMargin :: size_margin()) ->
    Bytes :: integer().
calc_bytes({megabytes, MegaBytes}) ->
    MegaBytes * 1048576.

-spec read_from_buckets(Buckets :: [shard_name()],
			Key :: term()) ->
    {ok, Value :: term()} | {error, Reason :: term()}.
read_from_buckets([], _) ->
    {error, not_found};
read_from_buckets([Bucket|Rest], Key) ->
    case enterdb_ldb_worker:read(Bucket, Key) of
	{ok, Value} ->
	    {ok, Value};
	{error, _Reason} ->
	    read_from_buckets(Rest, Key)
    end.

-spec delete_from_buckets(Buckets :: [shard_name()],
			  Key :: term()) ->
    ok | {error, Reason :: term()}.
delete_from_buckets(Buckets, Key) ->
    delete_from_buckets(Buckets, Key, []).

-spec delete_from_buckets(Buckets :: [shard_name()],
			  Key :: term(),
			  ErrAcc :: []) ->
    ok | {error, Reason :: term()}.
delete_from_buckets([], _, []) ->
    ok;
delete_from_buckets([], _, Error) ->
    {error, Error};
delete_from_buckets([Bucket|Rest], Key, ErrAcc) ->
    case enterdb_ldb_worker:delete(Bucket, Key) of
	ok ->
	    delete_from_buckets(Rest, Key, ErrAcc);
	{error, Reason} ->
	    delete_from_buckets(Rest, Key, [{Bucket, Reason} | ErrAcc])
    end.
