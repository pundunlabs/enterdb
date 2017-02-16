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
-export([start_link/1]).

%% API exports
-export([read/2,
	 write/4,
	 update/6,
	 delete/2,
	 read_range_binary/4,
	 read_range_n_binary/4,
	 close_shard/1,
	 delete_shard/1]).

%% Exports for self spawned process
-export([size_wrap/3,
	 time_wrap/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(entry, {key, value}).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-define(COUNTER_TRESHOLD, 10000).

-record(s, {tid, shard}).

%%%===================================================================
%%% API functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(#enterdb_stab{}) ->
    {ok, Pid :: pid} | ignore | {error, Error :: term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% Read Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec read(Shard :: string(),
	   Key :: binary()) ->
    {ok, Value :: term()} | {error, Reason :: term()}.
read(Shard, Key) ->
    Buckets = get_buckets(get_tid(Shard)),
    read_from_buckets(Buckets, Key).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Columns to given shard.
%% @end
%%--------------------------------------------------------------------
-spec write(Shard :: string(),
	    Wrapper :: #{num_of_buckets := pos_integer()},
            Key :: binary(),
            Columns :: binary()) ->
    ok | {error, Reason :: term()}.
write(Shard, W, Key, Columns) ->
    UpdateOp = {#entry.value, 1, ?COUNTER_TRESHOLD, 0},
    Tid = get_tid(Shard),
    Count =  ets:update_counter(Tid, '$counter', UpdateOp),
    SizeMargin = maps:get(size_margin, W, undefined),
    check_counter_wrap(Count, Tid, Shard, SizeMargin),
    [Bucket|_]  = get_buckets(Tid),
    enterdb_ldb_worker:write(Bucket, Key, Columns).

%%--------------------------------------------------------------------
%% @doc
%% Update Key according to Op on given shard.
%% @end
%%--------------------------------------------------------------------
-spec update(Shard :: string(),
             Key :: binary(),
             Op :: update_op(),
	     DataModel :: data_model(),
	     Mapper :: module(),
	     Dist :: boolean()) ->
    ok | {error, Reason :: term()}.
update(Shard, Key, Op, DataModel, Mapper, Dist) ->
    Buckets  = get_buckets(get_tid(Shard)),
    update_on_buckets(Buckets, Key, Op, DataModel, Mapper, Dist).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
	     Key :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Shard, Key) ->
    Buckets = get_buckets(get_tid(Shard)),
    delete_from_buckets(Buckets, Key).

%%--------------------------------------------------------------------
%% @doc
%% Read a range of keys from a given shard and return read
%% items in binary format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_binary(Shard :: string(),
			{StartKey :: binary(), StopKey :: binary()},
			Chunk :: pos_integer(),
			Dir :: 0 | 1) ->
    {ok, [{binary(), binary()}]} | {error, Reason :: term()}.
read_range_binary(Shard, Range, Chunk, Dir) ->
    Buckets = get_buckets(get_tid(Shard)),
    read_range_from_buckets(Buckets, Range, Chunk, Dir).

%%--------------------------------------------------------------------
%% @doc
%% Read N number of keys from a given shard and return read
%% items in binary format.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n_binary(Shard :: string(),
			  StartKey :: binary(),
			  N :: pos_integer(),
			  Dir :: 0 | 1) ->
    {ok, [{binary(), binary()}]} | {error, Reason :: term()}.
read_range_n_binary(Shard, StartKey, N, Dir) ->
    Buckets = get_buckets(get_tid(Shard)),
    read_range_n_from_buckets(Buckets, StartKey, N, Dir).

%%--------------------------------------------------------------------
%% @doc
%% Close leveldb workers and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec close_shard(Shard :: shard_name()) -> ok | {error, Reason :: term()}.
close_shard(Shard) ->
    Tid = get_tid(Shard),
    cancel_timer(Tid),
    Buckets = get_buckets(Tid),
    Res = [supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(B)) ||
	    B <- Buckets],
    enterdb_lib:check_error_response(lists:usort(Res)).

%%--------------------------------------------------------------------
%% @doc
%% Delete leveldb buckets and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec delete_shard(Args :: [term()]) -> ok | {error, Reason :: term()}.
delete_shard(Args) ->
    ESTAB = proplists:get_value(tab_rec, Args),
    Buckets = maps:get(buckets, ESTAB),
    [ begin
	NewArgs = lists:keyreplace(name, 1, Args, {name, Bucket}),
	ok = enterdb_ldb_worker:delete_db(NewArgs),
	supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(Bucket))
      end || Bucket <- Buckets],
    ok.

-spec size_wrap(Tid :: term(),
		Shard :: shard_name(),
		SizeMargin :: size_margin()) ->
    ok.
size_wrap(Tid, Shard, SizeMargin) ->
    [Bucket|_]  = get_buckets(Tid),
    {ok, Size} =  enterdb_ldb_worker:approximate_size(Bucket),
    case size_exceeded(Size, SizeMargin) of
	true ->
	    Pid = enterdb_ns:get(Shard),
	    gen_server:call(Pid, wrap);
	false ->
	    ok
    end.

-spec time_wrap(Shard :: shard_name()) ->
    ok.
time_wrap(Shard) ->
    Pid = enterdb_ns:get(Shard),
    ok = gen_server:call(Pid, wrap).

-spec get_tid(Shard :: string()) ->
    Tid :: term().
get_tid(Shard) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, get_tid).

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
init([Start, #{shard := Shard,
	       wrapper := Wrapper,
	       buckets := Buckets} = ESTAB]) ->
    ?info("Starting EnterDB LevelDB WRP Server for Shard ~p",[Shard]),
    Options = [public,{read_concurrency, true},{keypos, #entry.key}],
    Tid = ets:new(bucket, Options),
    enterdb_ns:register_pid(self(), Shard),
    register_buckets(Tid, Shard, Buckets),
    ChildArgs = enterdb_lib:get_ldb_worker_args(Start, ESTAB),
    [{ok, _} = supervisor:start_child(enterdb_ldb_sup,
	[lists:keyreplace(name, 1, ChildArgs, {name, Bucket})]) ||
	Bucket <- Buckets],
    ets:insert(Tid, #entry{key='$counter', value=0}),
    TimeMargin = maps:get(time_margin, Wrapper, undefined),
    register_timeout(Tid, Shard, TimeMargin),
    {ok, #s{tid=Tid, shard=Shard}};
init([Start, #{shard := Shard,
	       wrapper := #{num_of_buckets := N}} = ESTAB]) ->
    ?info("Creating EnterDB LevelDB WRP Server for Shard ~p",[Shard]),
    List = [lists:concat([Shard, "_", Index]) || Index <- lists:seq(0, N-1)],
    init([Start, ESTAB#{buckets => List}]).
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
handle_call(get_tid, _From, State) ->
    {reply, State#s.tid, State};
handle_call(wrap, _From, State = #s{tid=Tid, shard=Shard}) ->
    Buckets = get_buckets(Tid),
    LastBucket = lists:last(Buckets),
    ok = enterdb_ldb_worker:recreate_shard(LastBucket),
    WrappedBuckets = [LastBucket | lists:droplast(Buckets)],
    reset_timer(Tid, Shard),
    true = register_buckets(Tid, Shard, WrappedBuckets),
    {reply, ok, State};
handle_call({get_iterator, _Caller}, _From, State) ->
    {reply, {error, "not_supported"}, State}.

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
-spec register_buckets(Tid :: term(),
		       Shard :: string(),
		       Buckets :: [shard_name()]) ->
    true.
register_buckets(Tid, Shard, Buckets) ->
    ok = enterdb_lib:update_bucket_list(Shard, Buckets),
    ets:insert(Tid, #entry{key='$buckets', value=Buckets}).

-spec get_buckets(Tid :: term()) ->
    ok.
get_buckets(Tid) ->
    case ets:lookup(Tid, '$buckets') of
	[#entry{value=Value}] ->
	    Value;
	_ ->
	    {error, no_entry}
    end.

-spec register_timeout(Tid :: term(),
		       Shard :: string(),
		       TimeMargin :: time_margin()) -> ok.
register_timeout(_, _, undefined) ->
    ok;
register_timeout(Tid, Shard, TimeMargin) ->
    Time = calc_milliseconds(TimeMargin),
    {ok, Tref} = timer:apply_after(Time, ?MODULE, time_wrap, [Shard]),
    true = register_timer_ref(Tid, #{tref=>Tref, time_margin=>TimeMargin}),
    ok.

-spec register_timer_ref(Tid :: term(), Map :: map()) ->
    true.
register_timer_ref(Tid, Map) ->
    ets:insert(Tid, #entry{key='$reference', value=Map}).

-spec reset_timer(Tid :: term(), Shard :: string()) ->
    ok.
reset_timer(Tid, Shard) ->
    case ets:lookup(Tid, '$reference') of
	[] ->
	    ok;
	[#entry{value=Map}] ->
	    Tref = maps:get(tref, Map),
	    timer:cancel(Tref),
	    TimeMargin = maps:get(time_margin, Map),
	    register_timeout(Tid, Shard, TimeMargin)
    end.

-spec cancel_timer(Tid :: term()) ->
    ok.
cancel_timer(Tid) ->
    case ets:lookup(Tid, '$reference') of
	[] ->
	    ok;
	[#entry{value=Map}] ->
	    Tref = maps:get(tref, Map),
	    timer:cancel(Tref),
	    ets:delete(Tid, '$reference')
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
			 Tid :: term(),
			 Shard :: shard_name(),
			 SizeMargin :: size_margin()) ->
    ok.
check_counter_wrap(_, _, _, undefined) ->
    ok;
check_counter_wrap(Count, _, _, _) when Count < ?COUNTER_TRESHOLD ->
    ok;
check_counter_wrap(Count, Tid, Shard, SizeMargin)
    when Count == ?COUNTER_TRESHOLD ->
    erlang:spawn(?MODULE, size_wrap, [Tid, Shard, SizeMargin]).

-spec size_exceeded(Size :: integer(),
		    SizeMargin :: size_margin()) ->
    true | false.
size_exceeded(Size, SizeMargin) ->
    BytesMargin = calc_bytes(SizeMargin),
    Size > BytesMargin.

-spec calc_bytes(SizeMargin :: size_margin()) ->
    Bytes :: integer().
calc_bytes({gigabytes, Gigabytes}) ->
    Gigabytes * 1073741824;
calc_bytes({megabytes, Megabytes}) ->
    Megabytes * 1048576.

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

-spec read_range_from_buckets(Buckets :: [shard_name()],
			      {StartKey :: binary(), StopKey :: binary()},
			      Chunk :: pos_integer(),
			      Dir :: 0 | 1) ->
    {ok, [{binary(), binary()}], Cont :: complete | key()} |
    {error, Reason :: term()}.
read_range_from_buckets(Buckets,
			Range,
			Chunk, Dir) ->
    KVLs_and_Conts =
	[begin
	    {ok, KVL, Cont} =
		 enterdb_ldb_worker:read_range_binary(B, Range, Chunk),
	    {KVL, Cont}
	 end || B <- Buckets],
    {KVLs, Conts} = lists:unzip(KVLs_and_Conts),

    ContKeys = [K || K <- Conts, K =/= complete],

    case get_continuation(Dir, ContKeys) of
	complete ->
	    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls(Dir, KVLs),
	    KVL = unique(MergedKVL),
	    {ok, KVL, complete};
	{Cont, _} = ContKVP ->
	    CompKVLs = [[ContKVP]|KVLs],
	    {ok, SparseKVL} = leveldb_utils:merge_sorted_kvls(Dir, CompKVLs),
	    MergedKVL = enterdb_lib:cut_kvl_at(Cont, SparseKVL),
	    KVL = unique(MergedKVL),
	    {ok, KVL, Cont}
    end.

-spec get_continuation(Dir :: 0 | 1, ContKeys :: [binary()]) ->
    complete | {key(), binary()}.
get_continuation(_Dir, []) ->
    complete;
get_continuation(Dir, ContKeys) ->
    enterdb_lib:reduce_cont(Dir, ContKeys).

-spec read_range_n_from_buckets(Buckets :: [shard_name()],
				SKey :: binary(),
				N :: pos_integer(),
				Dir :: 0 | 1) ->
    {ok, [{binary(), binary()}]} | {error, Reason :: term()}.
read_range_n_from_buckets(Buckets, SKey, N, Dir) ->
    KVLs =
	[begin
	    {ok, KVL} = enterdb_ldb_worker:read_range_n_binary(B, SKey, N),
	    KVL
	 end || B <- Buckets],
    {ok, MergedKVL} = leveldb_utils:merge_sorted_kvls(Dir, KVLs),
    UniqueKVL = unique(MergedKVL),
    {ok, lists:sublist(UniqueKVL, N)}.

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

-spec update_on_buckets(Buckets :: [shard_name()],
			Key :: term(),
			Op :: update_op(),
			DataModel ::  data_model(),
			Mapper :: module(),
			Dist :: boolean()) ->
    {ok, Value :: binary()} | {error, Reason :: term()}.
update_on_buckets([], _, _, _, _, _) ->
    {error, not_found};
update_on_buckets([Bucket|Rest], Key, Op, DataModel, Mapper, Dist) ->
    case enterdb_ldb_worker:update(Bucket, Key, Op, DataModel, Mapper, Dist) of
	{ok, Value} ->
	    {ok, Value};
	{error, _} ->
	    update_on_buckets(Rest, Key, Op, DataModel, Mapper, Dist)
    end.

-spec unique([{DBKey :: binary(), DBVal :: binary()}]) ->
    [{binary(), binary()}].
unique(KVL) ->
    unique(KVL, []).

-spec unique([{DBKey :: binary(), DBVal :: binary()}],
	     Acc :: [{binary(), binary()}]) ->
    [{binary(), binary()}].
unique([], Acc) ->
    lists:reverse(Acc);
unique([{BK,BV}], Acc) ->
    lists:reverse([{BK,BV}|Acc]);
unique([{AK,AV}, {AK,_AVS} | Rest], Acc) ->
    unique([{AK,AV} | Rest], Acc);
unique([{AK,AV}, {BK,BV} | Rest], Acc) ->
    unique([{BK,BV} | Rest], [{AK,AV} | Acc]).
