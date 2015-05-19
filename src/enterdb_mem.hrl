%%%-------------------------------------------------------------------
%%% @author Jonas Falkevik
%%% @copyright (C) 2015, Jonas
%%% @doc
%%% enterdb_mem is handling the memory backed tables in enterdb.
%%% @end
%%%-------------------------------------------------------------------

-record(enterdb_mem_tab_config, {bucket_span, num_buckets}).

