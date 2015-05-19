%%%-------------------------------------------------------------------
%%% @author Jonas Falkevik
%%% @copyright (C) 2015, Jonas
%%% @doc
%%% enterdb_mem is handling the memory backed tables in enterdb.
%%% @end
%%%-------------------------------------------------------------------
-module(enterdb_mem).

-include("enterdb.hrl").

-export([init_tab/1,
	 init_tab/2]).

init_tab(#enterdb_table{name = Table, options = Options}) ->
    MemWrapped  = proplists:get_value(mem_wrapped, Options),
    [ init_mem_wrapped(Table, MemWrapped) || MemWrapped =/= undefined ].

init_tab(Table, Options) ->
    MemWrapped  = proplists:get_value(mem_wrapped, Options),
    [ init_mem_wrapped(Table, MemWrapped) || MemWrapped =/= undefined ].
    
%     case MemWrapped of
% 	Conf when Conf =/= undefined ->
% 	    init_mem_wrapped(Table, Conf);
% 	_ ->
% 	    ok
%     end.

init_mem_wrapped(Table, {BucketSize, NumBuckets}) ->
    enterdb_mem_wrp_mgr:init_tab(Table, {BucketSize, NumBuckets}).


