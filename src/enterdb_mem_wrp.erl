%%%-------------------------------------------------------------------
%%% @author Jonas Falkevik
%%% @copyright (C) 2015, Jonas
%%% @doc
%%% enterdb_mem_wrpd is handling the memory backed part of wrapped tables in enterdb.
%%% @end
%%%-------------------------------------------------------------------

-module(enterdb_mem_wrp).

-include("enterdb_mem.hrl").

-compile(export_all).
-define(mem_meta_tab, enterdb_mem_ets_meta_tab).

write(Tab, TS, Key, Data) ->
    TSBucket = get_bucket_ts(Tab, TS),
    write_(Tab, Key, Data, TSBucket).

write(Tab, TS, Key, Data, BucketSpan) ->
    TSBucket = calc_bucket(TS, BucketSpan),
    write_(Tab, Key, Data, TSBucket).

write_(Tab, Key, Data, TSBucket) ->
    case get_ets_table(Tab, TSBucket) of
	{error, _} = E ->
	    E;
	{_EtsTab, wrapping} ->
	    {error, wrapping};
	{EtsTab, _} ->
	    ets:insert(EtsTab, {Key, Data})
    end.

%% Bucket span is unknown first find the bucket span
read(Tab, TS, Key) ->
    TSBucket = get_bucket_ts(Tab, TS),
    read_(Tab, Key, TSBucket).

%% Bucket span known
read_bs(Tab, Key, BucketSpan)->
    TSBucket = calc_bucket(Tab, BucketSpan),
    read_(Tab, Key, TSBucket).

read_(Tab, Key, TSBucket) ->
    case get_ets_table(Tab, TSBucket) of
	{error, _} = E ->
	    E;
	{EtsTab, Info} ->
	    get_value(Key, ets:lookup(EtsTab, Key), Info)
    end.

get_value(Key, [{Key, V}], _) ->
    {ok, V};
get_value(_Key, [], _) ->
    {ok, []};
get_value(_,_, wrapping) ->
    {error, wrapping}.

%% Find the bucket span from meta data table
get_bucket_ts(Tab, TS) ->
    case catch ets:lookup(?mem_meta_tab, {Tab, config}) of
	[{_Key, #enterdb_mem_tab_config{bucket_span = BucketSpan}}] ->
	    calc_bucket(TS, BucketSpan);
	_ ->
	    {error, no_mem_tab_info}
    end.

get_ets_table(Tab, TSBucket) ->
    case ets:lookup(enterdb_mem_ets_meta_tab, Key = {Tab, TSBucket}) of
	[] ->
	    {error, no_mem_bucket};
	[{Key, {info, EtsTab, Info}}] ->
	    {EtsTab, Info};
	[{Key, EtsTab}] ->
	    {EtsTab, no_info}
    end.

calc_bucket(TS, BucketSpan) ->
    {Date, {H, M, _S}} = calendar:now_to_datetime(TS),
    {Date, {H, M - M rem BucketSpan, 0}}.

step_time(DateTime, MinutesStep) ->
    Sec = calendar:datetime_to_gregorian_seconds(DateTime),
    calendar:gregorian_seconds_to_datetime(Sec + MinutesStep * 60).


sec_to_next_span(BucketSpan) ->
    Now = os:timestamp(),
    CurrBucket = calc_bucket(Now, BucketSpan),
    CurrBucketSec = calendar:datetime_to_gregorian_seconds(CurrBucket),
    NextBucketSec = CurrBucketSec + BucketSpan * 60,
    CurrSec = calendar:datetime_to_gregorian_seconds(calendar:now_to_datetime(Now)),
    NextBucketSec - CurrSec.

