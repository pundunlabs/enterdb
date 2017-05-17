%%%-------------------------------------------------------------------
%%% @author Jonas Falkevik
%%% @copyright (C) 2015, Jonas
%%% @doc
%%% enterdb_mem is handling the memory backed tables in enterdb.
%%% @end
%%%-------------------------------------------------------------------
-module(enterdb_mem).

-include("enterdb.hrl").

-export([init_tab/1]).

init_tab(#{name := Table, mem_wrapper := MemWrapped}) ->
    init_mem_wrapper(Table, MemWrapped);
init_tab(_)->
    ok.

init_mem_wrapper(Table, {BucketSize, NumBuckets}) ->
    enterdb_mem_wrp_mgr:init_tab(Table, {BucketSize, NumBuckets}).
