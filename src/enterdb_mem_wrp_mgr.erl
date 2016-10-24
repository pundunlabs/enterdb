%%%-------------------------------------------------------------------
%%% @author Jonas Falkevik
%%% @copyright (C) 2015, Jonas
%%%-------------------------------------------------------------------

-module(enterdb_mem_wrp_mgr).

-include("enterdb_mem.hrl").
-include_lib("gb_log/include/gb_log.hrl").

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-export([start_link/0,
	 init_tab/2,
	 dump_tables/0,
	 reinit_tables/0]).

-record(state, {mem_info_tab, fenix, dump}).

-define(mem_meta_tab, enterdb_mem_ets_meta_tab).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_tab(Table, Args) ->
    gen_server:call(?MODULE, {init_tab, {Table, Args}}).

dump_tables() ->
    gen_server:call(?MODULE, dump_tables).

reinit_tables() ->
    gen_server:call(?MODULE, reinit_tables).

init([]) ->
    InfoTab = ets:new(?mem_meta_tab,
		        [public,
			 named_table,
			 {read_concurrency, true}]),

    {ok, #state{mem_info_tab = InfoTab, fenix=now(), dump=false}}.

handle_call({init_tab, {Table, Args}}, _From, State) ->
    open_table(Table, Args, State),
    {reply, ok, State};

handle_call(dump_tables, _From, State) ->
    spawn(fun() -> do_dump_tables() end),
    {reply, ok, State#state{dump=true}};

handle_call(reinit_tables, _From, State = #state{dump=true}) ->
    spawn(fun() -> do_reinit_tables() end),
    {reply, ok, State#state{fenix=now(), dump=false}};

handle_call(_Call, _From, State) ->
    {reply, nok, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({wrap, Table, Fenix}, State = #state{fenix=Fenix, dump=false}) ->
    spawn(fun() -> wrap(Table, Fenix) end),
    {noreply, State};

handle_info({wrap,_,_}, State) ->
    ?debug("ignore wrap", []),
    {noreply, State};
handle_info(Info, State) ->
    ?debug("info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal Functions
wrap(Table, Fenix) ->
    ?debug("wrapping ~p", [Table]),
    #enterdb_mem_tab_config{bucket_span=BSpan, num_buckets=_NumB} = get_config(Table),
    create_new_bucket(Table, BSpan),
    move_out_old_bucket(Table, BSpan),
    schedule_next_wrap(Table, BSpan, Fenix).

move_out_old_bucket(Table, BSpan) ->
    case ets:lookup(?mem_meta_tab, {Table, last}) of 
	[{{Table, last}, TSBucket}] ->
	    NextTSBucket = enterdb_mem_wrp:step_time(TSBucket, BSpan),
	    ets:insert(?mem_meta_tab, {{Table, last}, NextTSBucket}),
	    {Ets, _} = get_bucket_table(Table, TSBucket),
	    ?debug("~p last was ~p and ets ~p", [Table, TSBucket, Ets]),
	    spawn(fun() -> write_bucket_to_backend(Table, Ets, TSBucket) end);
	_ ->
	    throw(error_no_last_in_wrap)
    end.

write_bucket_to_backend(Table, Ets, TSBucket) ->
    Elements = ets:info(Ets, size),
    Startdump = os:timestamp(),
    %% indicate that the table should not be written to
    ets:insert(?mem_meta_tab, {{Table, TSBucket}, {info, Ets, wrapping}}),
    do_write_bucket_to_backend(Table, ets:select(Ets, [{'_',[],['$_']}], 10000)),
    Stopdump  = os:timestamp(),
    Time = timer:now_diff(Stopdump, Startdump) / 1000000,
    ?debug("dumped ~p ~p with ~p elements (~ps)", [Table, TSBucket, Elements, trunc(Time)]),
    ets:delete(?mem_meta_tab, {Table, TSBucket}),
    ets:delete(Ets).
    
do_write_bucket_to_backend(_Table, '$end_of_table') ->
    ok;
do_write_bucket_to_backend(Table, {Data, Cont}) ->
    do_write_data(Table, Data),
    do_write_bucket_to_backend(Table, ets:select(Cont)).

do_write_data(Table, [{Key,Value}| Rest]) ->
    enterdb:write_to_disk(Table, Key, Value),
    do_write_data(Table, Rest);
do_write_data(_, []) ->
    ok.

get_bucket_table(Tab, TSBucket) ->
    case ets:lookup(?mem_meta_tab, Key = {Tab, TSBucket}) of
	[] ->
	    {error, no_mem_bucket};
	[{Key, {info, EtsTab, Info}}] ->
	    {EtsTab, Info};
	[{Key, EtsTab}] ->
	    {EtsTab, no_info}
    end.

create_new_bucket(Table, Bspan) ->
    case ets:lookup(?mem_meta_tab, {Table, first}) of
	[{{Table, first}, Bucket}] ->
	    NewBucket = enterdb_mem_wrp:step_time(Bucket, Bspan),
	    ?debug("old first ~p new first ~p", [Bucket, NewBucket]),
	    NewEts = do_open_ets_table(Table, NewBucket),
	    ets:give_away(NewEts, erlang:whereis(?MODULE), new_bucket),
	    ets:insert(?mem_meta_tab, {{Table, first}, NewBucket});
	_ ->
	    throw(error_enter_db_mem_no_first_bucket)
    end.

get_config(Table) ->
    case ets:lookup(?mem_meta_tab, {Table, config}) of
	[{{Table, config}, Cfg}] ->
	    Cfg;
	R ->
	    R
    end.

schedule_next_wrap(Table, BucketSpan, Fenix) ->
    SecToNextWrap = enterdb_mem_wrp:sec_to_next_span(BucketSpan),
    random:seed(now()),
    SendAfter = SecToNextWrap * 1000 + random:uniform(20000),
    ?debug("schedule next mem wrap for ~p in ~p sec", [Table, SendAfter / 1000]),
    erlang:send_after(SendAfter, ?MODULE, {wrap, Table, Fenix}).

open_table(Table, {BucketSize, NumBuckets}, State) ->
    Current = enterdb_mem_wrp:calc_bucket(os:timestamp(), BucketSize),
    
    FutureSteps = lists:seq(BucketSize, BucketSize * 4, BucketSize),
    FutureBuckets = [ enterdb_mem_wrp:step_time(Current, FTS) || FTS <- FutureSteps ],
    ?debug("~p futurebuckets ~p", [Table, FutureBuckets]),
    HistorySteps = lists:seq(-BucketSize * (NumBuckets-1), 0, BucketSize),
    HistoryBuckets = [ enterdb_mem_wrp:step_time(Current, HTS) || HTS <- HistorySteps ],
    ?debug("~p historybuckets ~p", [Table, HistoryBuckets]),
    open_ets_tables(Table, lists:flatten(FutureBuckets ++ HistoryBuckets)),
    
    % insert pointer to first and last bucket
    ets:insert(?mem_meta_tab, {{Table, first}, lists:last(FutureBuckets)}),
    ets:insert(?mem_meta_tab, {{Table, last}, hd(HistoryBuckets)}),

    Config = #enterdb_mem_tab_config{bucket_span = BucketSize, num_buckets = NumBuckets},
    ets:insert(?mem_meta_tab, {{Table, config}, Config}),
    
    %% schedule next wrap callback
    schedule_next_wrap(Table, BucketSize, State#state.fenix).

open_ets_tables(Table, [Bucket| Rest]) ->
    do_open_ets_table(Table, Bucket),
    open_ets_tables(Table, Rest);
open_ets_tables(_, []) ->
    ok.

do_open_ets_table(Tab, Bucket) ->
    %% name is not important of the table
    EtsID = 
	ets:new(enterdb_mem_wrp_, [public,
				   {read_concurrency, true},
				   {write_concurrency, true}]),
    true = ets:insert(?mem_meta_tab, {{Tab, Bucket}, EtsID}),
    EtsID.

do_dump_tables() ->
    Tabs = ets:tab2list(?mem_meta_tab),
    do_dump_tables(Tabs).

%% found ets table, lets dump it to disk
do_dump_tables([{{Tab,TSBucket}, Ets}| Rest]) when is_integer(Ets) ->
    write_bucket_to_backend(Tab, Ets, TSBucket),
    do_dump_tables(Rest);
%% ignore meta data that does not include any ets table
do_dump_tables([_|Rest]) ->
    do_dump_tables(Rest);
do_dump_tables([]) ->
    ?debug("done dumping mem tables to disk",[]),
    ok.

do_reinit_tables() ->
    Tabs = ets:tab2list(?mem_meta_tab),
    do_reinit_tables(Tabs).

%% found config for tab, lets reinit
do_reinit_tables([{{Tab,config}, #enterdb_mem_tab_config{} = C} | Rest]) ->
    #enterdb_mem_tab_config{bucket_span=BSpan, num_buckets=NumB} = C,
    init_tab(Tab, {BSpan,NumB}),
    do_reinit_tables(Rest);
%% ignore first and last meta data
do_reinit_tables([_|Rest]) ->
    do_reinit_tables(Rest);
do_reinit_tables([]) ->
    ok.
