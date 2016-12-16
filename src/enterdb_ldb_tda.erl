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


-module(enterdb_ldb_tda).

-behaviour(gen_server).

%% gen_server API
-export([start_link/1]).

%% API exports
-export([read/4,
	 write/5,
	 update/4,
	 delete/4,
	 read_range_binary/4,
	 read_range_n_binary/4,
	 close_shard/1,
	 delete_shard/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(entry, {key, value}).
-record(s, {tid}).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

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
	   Tda :: tda(),
	   Key :: key(),
	   DBKey :: binary()) ->
    {ok, Value :: term()} | {error, Reason :: term()}.
read(Shard, Tda = #{ts_field := KF}, Key, DBKey) ->
    read_(Shard, Tda, find_timestamp_in_key(KF, Key), DBKey).

-spec read_(Shard :: string(),
	    Tda :: tda(),
	    Ts :: undefined | integer(),
	    DBKey :: binary()) ->
    {ok, Value :: term()} | {error, Reason :: term()}.
read_(Shard, #{num_of_buckets := S,
	       time_margin := {_, _} = TM,
	       precision := P}, Ts, DBKey) when is_integer(Ts) -> 
    N = get_nanoseconds(P, Ts) div get_nanoseconds(TM),
    BucketId = N rem S,
    Tid = get_tid(Shard),
    enterdb_ldb_worker:read(get_bucket(Tid, BucketId), DBKey).

%%--------------------------------------------------------------------
%% @doc
%% Write Key/Columns to given shard.
%% @end
%%--------------------------------------------------------------------
-spec write(Shard :: string(),
	    Tda :: tda(),
            Key :: key(),
            DBKey :: binary(),
            DBColumns :: binary()) ->
    ok | {error, Reason :: term()}.
write(Shard, Tda = #{ts_field := KF}, Key, DBKey, DBColumns) ->
    write_(Shard, Tda, find_timestamp_in_key(KF, Key), DBKey, DBColumns).

write_(Shard, #{num_of_buckets := S,
	       time_margin := {_, _} = TM,
	       precision := P}, Ts, DBKey, DBColumns) when is_integer(Ts) ->
    N = get_nanoseconds(P, Ts) div get_nanoseconds(TM),
    BucketId = N rem S,
    Tid = get_tid(Shard),
    {Old, Bucket} = get_bucket_n(Tid, BucketId),
    ok = wrap(Shard, N, Old, BucketId),
    enterdb_ldb_worker:write(Bucket, DBKey, DBColumns).

%%--------------------------------------------------------------------
%% @doc
%% Update Key according to Op on given shard.
%% @end
%%--------------------------------------------------------------------
-spec update(TD :: #{},
             Key :: key(),
             DBKey :: binary(),
             Op :: update_op()) ->
    ok | {error, Reason :: term()}.
update(TD = #{tda :=  #{ts_field := KF}}, Key, DBKey, Op) ->
    update_(TD, find_timestamp_in_key(KF, Key), DBKey, Op).

-spec update_(#{},
	      Ts :: integer(),
	      DBKey :: binary(),
	      Op :: update_op()) ->
    ok | {error, Reason :: term()}.
update_(#{shard := Shard,
	  tda := #{num_of_buckets := S,
		  time_margin := {_, _} = TM,
		  precision := P},
	  data_model := DataModel,
	  column_mapper := Mapper,
	  distributed := Dist},
       Ts, DBKey, Op) when is_integer(Ts) ->
    N = get_nanoseconds(P, Ts) div get_nanoseconds(TM),
    BucketId = N rem S,
    Tid = get_tid(Shard),
    {Old, Bucket}  = get_bucket_n(Tid, BucketId),
    ok = wrap(Shard, N, Old, BucketId),
    enterdb_ldb_worker:update(Bucket, DBKey, Op, DataModel, Mapper, Dist).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from given wrapped shard.
%% @end
%%--------------------------------------------------------------------
-spec delete(Shard :: string(),
	     Tda :: tda(),
	     Key :: key(),
	     DBKey :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Shard, Tda = #{ts_field := KF}, Key, DBKey) ->
    delete_(Shard, Tda, find_timestamp_in_key(KF, Key), DBKey).

delete_(Shard, #{num_of_buckets := S,
		 time_margin := {_, _} = TM,
		 precision := P}, Ts, DBKey) when is_integer(Ts) ->
    N = get_nanoseconds(P, Ts) div get_nanoseconds(TM),
    BucketId = N rem S,
    Tid = get_tid(Shard),
    Bucket  = get_bucket(Tid, BucketId),
    enterdb_ldb_worker:delete(Bucket, DBKey).

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
    Buckets = get_bucket_list(get_tid(Shard)),
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
    Buckets = get_bucket_list(get_tid(Shard)),
    read_range_n_from_buckets(Buckets, StartKey, N, Dir).

%%--------------------------------------------------------------------
%% @doc
%% Close leveldb workers and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec close_shard(Shard :: shard_name()) -> ok | {error, Reason :: term()}.
close_shard(Shard) ->
    BucketList = get_bucket_list(get_tid(Shard)),
    Res = [supervisor:terminate_child(enterdb_ldb_sup, enterdb_ns:get(B)) ||
	    B <- BucketList],
    enterdb_lib:check_error_response(lists:usort(Res)).

%%--------------------------------------------------------------------
%% @doc
%% Delete leveldb buckets and clear helper ets tables.
%% @end
%%--------------------------------------------------------------------
-spec delete_shard(Args :: [term()]) -> ok | {error, Reason :: term()}.
delete_shard(Args) ->
    ESTAB = proplists:get_value(tab_rec, Args),
    BucketList = get_bucket_list(get_tid(ESTAB#enterdb_stab.shard)),
    delete_shard(Args, BucketList).

-spec delete_shard(Args :: [term()],
		   BucketList :: [string()] | undefined) ->
    ok | {error, Reason :: term()}.
delete_shard(_Args, undefined) ->
    {error, "buckets_not_found"};
delete_shard(Args, BucketList) ->
    [begin
	  NewArgs = lists:keyreplace(name, 1, Args, {name, Bucket}),
	  ok = enterdb_ldb_worker:delete_db(NewArgs)
     end || Bucket <- BucketList],
    ok.

-spec wrap(Shard :: string(),
	   N :: pos_integer(),
	   Old :: pos_integer(),
	   BucketId :: integer()) ->
    ok.
wrap(_, N, N, _) ->
    ok;
wrap(_, N, Old, _) when Old > N ->
    ok;
wrap(Shard, N, _, BucketId) ->
    Pid = enterdb_ns:get(Shard),
    gen_server:call(Pid, {wrap, N, BucketId}).

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
init([Start, #{shard := Shard, buckets := Buckets} = ESTAB]) ->
    ?info("Starting EnterDB LevelDB TDA Server for Shard ~p",[Shard]),
    Options = [public,{read_concurrency, true},{keypos, #entry.key}],
    Tid = ets:new(bucket, Options),
    enterdb_ns:register_pid(self(), Shard),
    register_bucket_list(Tid, Buckets),
    ChildArgs = enterdb_lib:get_ldb_worker_args(Start, ESTAB),
    [{ok, _} = supervisor:start_child(enterdb_ldb_sup,
	[lists:keyreplace(name, 1, ChildArgs, {name, Bucket})]) ||
	Bucket <- Buckets],
    {ok, #s{tid = Tid}};
init([Start, #{shard := Shard, tda := #{num_of_buckets := N}} = ESTAB]) ->
    ?info("Creating EnterDB LevelDB TDA Server for Shard ~p",[Shard]),
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
handle_call({wrap, N, BucketId}, _From, State) ->
    case get_bucket_n(State#s.tid, BucketId) of
	{O, Bucket} when O < N ->
	    ?debug("Wrapping tda ~p: ~p -> ~p", [BucketId, O, N]),
	    ok = enterdb_ldb_worker:recreate_shard(Bucket),
	    true = register_bucket(State#s.tid, BucketId, N, Bucket);
	{O, _} ->
	    ?debug("Not Wrapping tda ~p: ~p -> ~p", [BucketId, O, N]),
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
-spec register_bucket(Tid :: pid(),
		      BucketId :: integer(),
		      N :: pos_integer(),
		      Bucket :: string()) ->
    true.
register_bucket(Tid, BucketId, N, Bucket) ->
    ets:insert(Tid, #entry{key=BucketId, value={N, Bucket}}).

-spec register_bucket_list(Tid :: pid(),
			   BucketList :: [shard_name()]) ->
    true.
register_bucket_list(Tid, BucketList) ->
    register_bucket_list(Tid, BucketList, 0).

register_bucket_list(Tid, [Bucket|Rest], BucketId) ->
    ets:insert(Tid, #entry{key=BucketId, value={0, Bucket}}),
    register_bucket_list(Tid, Rest, BucketId+1);
register_bucket_list(_Tid, [], _) ->
    true.

-spec get_bucket_list(Tid :: pid()) ->
    [Bucket :: string()].
get_bucket_list(Tid) ->
    ets:foldl(fun(#entry{value = {_,B}}, Acc) -> [B | Acc] end, [], Tid).

-spec get_bucket(Tid :: pid(), BucketId :: integer()) ->
    Bucket :: string().
get_bucket(Tid, BucketId) ->
    case ets:lookup(Tid, BucketId) of
	[#entry{value={_, Bucket}}] ->
	    Bucket;
	_ ->
	    {error, no_entry}
    end.

-spec get_bucket_n(Tid :: pid(), BucketId :: integer()) ->
    {N :: undefined | pos_integer(), Bucket :: string()}.
get_bucket_n(Tid, BucketId) ->
    case ets:lookup(Tid, BucketId) of
	[#entry{value=Value}] ->
	    Value;
	_ ->
	    {error, no_entry}
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

-spec find_timestamp_in_key(TsField :: string(),
			    Key :: [{string(), term()}]) ->
    undefined | {ok, Ts :: timestamp()}.
find_timestamp_in_key(_, [])->
    undefined;
find_timestamp_in_key(TsField, [{TsField, Ts}|_Rest]) ->
    Ts;
find_timestamp_in_key(TsField, [_|Rest]) ->
    find_timestamp_in_key(TsField, Rest).

-spec get_nanoseconds(TimeMargin :: time_margin()) ->
    pos_integer() | {error, Reason :: term()}.
get_nanoseconds({seconds, S}) -> S * 1000000000;
get_nanoseconds({minutes, M}) -> M * 60000000000;
get_nanoseconds({hours, H}) -> H * 3600000000000;
get_nanoseconds(TM) -> {error, {time_margin, TM}}.

-spec get_nanoseconds(P :: time_unit(), Ts :: integer()) ->
    integer() | {error, Reason :: term()}.
get_nanoseconds(second, Ts) -> Ts * 1000000000;
get_nanoseconds(millisecond, Ts) -> Ts * 1000000;
get_nanoseconds(microsecond, Ts) -> Ts * 1000;
get_nanoseconds(nanosecond, Ts) -> Ts;
get_nanoseconds(P, _Ts) -> {error, {precision, P}}.
