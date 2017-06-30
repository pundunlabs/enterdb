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
-type field_name() :: string().
-type key() :: [{field_name(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {field_name(), term()}.

-type update_threshold() :: pos_integer().
-type update_setvalue() :: pos_integer().
-type update_instruction() :: increment |
			      {increment, update_threshold(), update_setvalue()} |
			      overwrite.
-type update_data() :: pos_integer() | term().
-type update_default() :: pos_integer() | term().
-type update_op() :: [
		      {field_name(), update_instruction(), update_data()}|
		      {field_name(), update_instruction(), update_data(), update_default()}
		     ].

-type type() :: leveldb |
		mem_leveldb |
		leveldb_wrapped |
		mem_leveldb_wrapped |
		leveldb_tda |
		mem_leveldb_tda |
		rocksdb.

-type data_model() :: kv | array | map.

-type time_margin() :: {seconds, pos_integer()} |
		       {minutes, pos_integer()} |
		       {hours, pos_integer()} |
		       undefined.

-type size_margin() :: {megabytes, pos_integer()} |
		       {gigabytes, pos_integer()} |
		       undefined.

-type time_unit() :: second | millisecond | microsecond | nanosecond.

-type comparator() :: ascending | descending.

-type hashing_method() :: virtual_nodes | consistent | uniform | rendezvous.

-type wrapper() :: #{num_of_buckets := pos_integer(),
		     time_margin => time_margin(),
		     size_margin => size_margin()}.

-type tda() :: #{num_of_buckets := pos_integer(),
		 time_margin := time_margin(),
		 ts_field := string(),
		 precision := time_unit()}.

-type ttl() :: pos_integer().

%% bucket_span and num_buckets are used by mem_wrapper.
%% We keep these seperate since the design of disk based wrapping
%% is changed and diversed.
-type bucket_span() :: pos_integer().
-type num_buckets() :: pos_integer().


-type table_option() :: [{type, type()} |
                         {data_model, data_model()} |
			 {wrapper, wrapper()} |
			 {mem_wrapper, {bucket_span(), num_buckets()}} |
			 {comparator, comparator()} |
			 {time_series, boolean()} |
			 {num_of_shards, pos_integer()} |
			 {distributed, boolean()} |
			 {replication_factor, pos_integer()} |
			 {hash_exclude, [string()]} |
			 {hashing_method, hashing_method()} |
			 {tda, tda()} |
			 {ttl, ttl()}].

-type timestamp() :: {pos_integer(),  %% mega seconds &
		      pos_integer(),  %% seconds &
		      pos_integer()}. %% micro seconds since start of epoch(UNIX)

-type node_name() :: atom().
-type shard_name() :: string().

-type shards() :: [{shard_name(), map()}] |
		  [shard_name()].

-type it() :: binary().

-type char_filter() :: nfc | nfd | nfkc | nfkd |
		       {M :: module(), F :: atom(), A :: [any()]} |
		       undefined.

-type tokenizer() :: unicode_word_boundaries |
		     {M :: module(), F :: atom(), A :: [any()]} |
		     undefined.

-type token_transform() :: lowercase | uppercase | casefold |
			   {M :: module(), F :: atom(), A :: [any()]} |
			   undefined.

-type token_add() :: {M :: module(), F :: atom(), A :: [any()]} |
		     undefined.

-type token_delete() :: english_stopwords |
			lucene_stopwords |
			wikipages_stopwords |
			{M :: module(), F :: atom(), A :: [any()]} |
			undefined.

-type token_filter() :: #{transform := token_transform(),
			add := token_add() | [token_add()],
			delete := token_delete() | [token_delete()]} |
			undefined.

-type index_options() :: #{char_filter := char_filter(),
			 tokenizer := tokenizer(),
			 token_filter := token_filter()}.

-record(enterdb_table, {name :: string(),
			map :: #{}}).

-record(enterdb_stab, {shard :: string(),
		       map :: #{}}).

-record(enterdb_ldb_resource, {name :: shard_name(),
			       resource :: binary()
			      }).
