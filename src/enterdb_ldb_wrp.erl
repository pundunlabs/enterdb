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
-export([size_wrap/2,
	 time_wrap/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-define(COUNTER_TRESHOLD, 10000).

-record(s, {shard}).
-record(pts, {buckets, counter, reference}).

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
    Pid = enterdb_ns:get(Shard),
    read_(Pid, Key).

read_(Pid, Key) when is_pid(Pid) ->
    Buckets = get_buckets(Pid),
    read_from_buckets(Buckets, Key);
read_(_, _) ->
    {error, "table_closed"}.

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
    Pid = enterdb_ns:get(Shard),
    write_(Pid, W, Key, Columns).

write_(Pid, W, Key, Columns) when is_pid(Pid) ->
    Count = enterdb_pts:update_counter(Pid, #pts.counter, 1, ?COUNTER_TRESHOLD, 0),
    SizeMargin = maps:get(size_margin, W, undefined),
    check_counter_wrap(Count, Pid, SizeMargin),
    [Bucket|_]  = get_buckets(Pid),
    enterdb_ldb_worker:write(Bucket, Key, Columns);
write_(_, _, _, _) ->
    {error, "table_closed"}.

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
    Pid = enterdb_ns:get(Shard),
    update_(Pid, Key, Op, DataModel, Mapper, Dist).

update_(Pid, Key, Op, DataModel, Mapper, Dist) when is_pid(Pid) ->
    Buckets  = get_buckets(Pid),
    update_on_buckets(Buckets, Key, Op, DataModel, Mapper, Dist);
update_(_, _, _, _, _, _) ->
    {error, "table_closed"}.

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
	     Key :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Shard, Key) ->
    Pid = enterdb_ns:get(Shard),
    delete_(Pid, Key).

delete_(Pid, Key) when is_pid(Pid) ->
    Buckets = get_buckets(Pid),
    delete_from_buckets(Buckets, Key);
delete_(_,_) ->
    {error, "table_closed"}.

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
    Pid = enterdb_ns:get(Shard),
    read_range_binary_(Pid, Range, Chunk, Dir).

read_range_binary_(Pid, Range, Chunk, Dir) when is_pid(Pid) ->
    Buckets = get_buckets(Pid),
    read_range_from_buckets(Buckets, Range, Chunk, Dir);
read_range_binary_(_, _, _, _) ->
    {error, "table_closed"}.

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
    Pid = enterdb_ns:get(Shard),
    read_range_n_binary_(Pid, StartKey, N, Dir).

read_range_n_binary_(Pid, StartKey, N, Dir) when is_pid(Pid) ->
    Buckets = get_buckets(Pid),
    read_range_n_from_buckets(Buckets, StartKey, N, Dir);
read_range_n_binary_(_, _, _, _) ->
    {error, "table_closed"}.

%%--------------------------------------------------------------------
%% @doc
%% Close leveldb worker processes.
%% @end
%%--------------------------------------------------------------------
-spec close_shard(Shard :: shard_name()) -> ok | {error, Reason :: term()}.
close_shard(Shard) ->
    Pid = enterdb_ns:get(Shard),
    cancel_timer(Pid),
    Buckets = get_buckets(Pid),
    Res = [supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(B)) ||
	    B <- Buckets],
    enterdb_lib:check_error_response(lists:usort(Res)).

%%--------------------------------------------------------------------
%% @doc
%% Delete leveldb buckets.
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

-spec size_wrap(Pid :: pid(),
		SizeMargin :: size_margin()) ->
    ok.
size_wrap(Pid, SizeMargin) ->
    [Bucket|_]  = get_buckets(Pid),
    {ok, Size} =  enterdb_ldb_worker:approximate_size(Bucket),
    case size_exceeded(Size, SizeMargin) of
	true ->
	    gen_server:call(Pid, wrap);
	false ->
	    ok
    end.

-spec time_wrap(Pid :: pid()) ->
    ok.
time_wrap(Pid) ->
    ok = gen_server:call(Pid, wrap).

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
    Pid = self(),
    enterdb_ns:register_pid(Pid, Shard),
    register_buckets(Shard, Buckets),
    ChildArgs = enterdb_lib:get_ldb_worker_args(Start, ESTAB),
    [{ok, _} = supervisor:start_child(enterdb_ldb_sup,
	[lists:keyreplace(name, 1, ChildArgs, {name, Bucket})]) ||
	Bucket <- Buckets],
    TimeMargin = maps:get(time_margin, Wrapper, undefined),
    Reference = register_timeout(Pid, TimeMargin),
    ok = enterdb_pts:new(Pid, #pts{buckets = Buckets,
				   counter = 0,
				   reference = Reference}),
    {ok, #s{shard=Shard}};
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
handle_call(wrap, _From, State = #s{shard=Shard}) ->
    Pid = self(),
    Buckets = get_buckets(Pid),
    LastBucket = lists:last(Buckets),
    ok = enterdb_ldb_worker:recreate_shard(LastBucket),
    WrappedBuckets = [LastBucket | lists:droplast(Buckets)],
    Reference = reset_timer(Pid),
    register_buckets(Shard, WrappedBuckets),
    enterdb_pts:overwrite(Pid, #pts{buckets = WrappedBuckets,
				      counter = 0,
				      reference = Reference}),
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
-spec register_buckets(Shard :: string(),
		       Buckets :: [shard_name()]) ->
    ok.
register_buckets(Shard, Buckets) ->
    ok = enterdb_lib:update_bucket_list(Shard, Buckets).

-spec get_buckets(Pid :: pid()) ->
    ok.
get_buckets(Pid) ->
    enterdb_pts:lookup(Pid, #pts.buckets).

-spec register_timeout(Pid :: term(),
		       TimeMargin :: time_margin()) -> ok.
register_timeout(_, undefined) ->
    undefined;
register_timeout(Pid, TimeMargin) ->
    Time = calc_milliseconds(TimeMargin),
    {ok, Tref} = timer:apply_after(Time, ?MODULE, time_wrap, [Pid]),
    #{tref => Tref, time_margin => TimeMargin}.

-spec reset_timer(Pid :: pid()) ->
    ok.
reset_timer(Pid) ->
    case enterdb_pts:lookup(Pid, #pts.reference) of
	undefined ->
	    undefined;
	Map ->
	    Tref = maps:get(tref, Map),
	    timer:cancel(Tref),
	    TimeMargin = maps:get(time_margin, Map),
	    register_timeout(Pid, TimeMargin)
    end.

-spec cancel_timer(Pid :: pid()) ->
    ok.
cancel_timer(Pid) ->
    case enterdb_pts:lookup(Pid, #pts.reference) of
	undefined ->
	    ok;
	Map ->
	    Tref = maps:get(tref, Map),
	    timer:cancel(Tref)
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
			 Pid :: pid(),
			 SizeMargin :: size_margin()) ->
    ok.
check_counter_wrap(_, _, undefined) ->
    ok;
check_counter_wrap(Count, _, _) when Count < ?COUNTER_TRESHOLD ->
    ok;
check_counter_wrap(Count, Pid, SizeMargin)
    when Count == ?COUNTER_TRESHOLD ->
    erlang:spawn(?MODULE, size_wrap, [Pid, SizeMargin]).

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
