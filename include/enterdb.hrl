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
%%
%% Enterdb records and macro definitions.
%%%===================================================================


-define(MAX_TABLE_NAME_LENGTH, 64).

-type key() :: [{string(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {string(), term()}.

-type type() :: leveldb |
		ets_leveldb |
		leveldb_wrapped |
		ets_leveldb_wrapped.

-type data_model() :: binary | array | hash.

-type time_margin() :: {seconds, pos_integer()} |
		       {minutes, pos_integer()} |
		       {hours, pos_integer()} |
		       undefined.

-type size_margin() :: {megabytes, pos_integer()} |
		       undefined.

-type comparator() :: ascending | descending.

-record(enterdb_wrapper, {num_of_buckets :: pos_integer(),
			  time_margin :: time_margin(),
			  size_margin :: size_margin()
			 }).
-record(enterdb_cluster, {name :: string(),
			  replication_factor :: pos_integer()
			 }).

%% bucket_span and num_buckets are used by mem_wrapper.
%% We keep these seperate since the design of disk based wrapping
%% is changed and diversed.
-type bucket_span() :: pos_integer().
-type num_buckets() :: pos_integer().

-type table_option() :: [{type, type()} |
                         {data_model, data_model()} |
			 {wrapper, #enterdb_wrapper{}} |
			 {mem_wrapper, {bucket_span(), num_buckets()}} |
			 {comparator, comparator()} |
			 {time_series, boolean()} |
			 {shards, pos_integer()} |
			 {distributed, boolean()} |
			 {replication_factor, pos_integer()} |
			 {clusters, [#enterdb_cluster{}]}].

-type timestamp() :: {pos_integer(),  %% mega seconds &
		      pos_integer(),  %% seconds &
		      pos_integer()}. %% micro seconds since start of epoch(UNIX)

-type op() :: first | last | {seek, key()} | next | prev.
-type it() :: binary().

-type node_name() :: atom().
-type shard_name() :: string().

-type shards() :: [{shard_name(), map()}] |
		  [shard_name()].

-record(enterdb_shard, {name :: shard_name(),
			node :: atom()}).

-record(enterdb_table, {name :: string(),
                        path :: string(),
                        key :: [string()],
                        columns :: [string()],
                        indexes :: [string()],
			comparator :: comparator(),
                        type	:: type(),
			data_model :: data_model(),
			distributed :: boolean(),
			options :: [table_option()],
                        shards :: shards()}).
%% enterdb shard tab
-record(enterdb_stab, {shard :: shard_name(),
		       name :: string(),
		       type :: type(),
		       key :: [string()],
		       columns :: [string()],
		       indexes :: [string()],
		       comparator :: comparator(),
		       data_model :: data_model(),
		       wrapper :: #enterdb_wrapper{},
		       buckets :: [shard_name()]}).

-record(enterdb_ldb_resource, {name :: shard_name(),
			       resource :: binary()
			      }).

-record(enterdb_lit_resource, {name :: shard_name(),
			       pid :: pid(),
			       it :: binary()
			     }).

-define(dyno, gb_dyno_dist).
