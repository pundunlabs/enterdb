%% Enterdb records and macro definitions.

-define(MAX_TABLE_NAME_LENGTH, 64).

-type key() :: [{atom(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {atom(), term()}.

-type backend() :: leveldb | ets_leveldb.
-type data_model() :: binary | array | hash.

-type file_margin() :: pos_integer().
-type time_margin() :: pos_integer().
-type bucket_size() :: pos_integer().
-type num_buckets() :: pos_integer().
-type mem_wrapper() :: {bucket_size(), num_buckets()}.
-type wrapper()	    :: {file_margin(), time_margin()}.

-type table_option() :: [{time_ordered, boolean()} |
                         {wrapped, wrapper()} |
			 {mem_wrapped, mem_wrapper()} |
			 {backend, backend()} |
                         {data_model, data_model()} |
			 {shards, integer()} |
			 {nodes, [atom()]}].

-type timestamp() :: {pos_integer(),  %% mega seconds &
		      pos_integer(),  %% seconds &
		      pos_integer()}. %% micro seconds since start of epoch(UNIX)

-type op() :: first | last | {seek, key()} | next | prev.
-type it() :: binary().
-type comparator() :: ascending | descending.

-record(enterdb_shard, {name :: string(),
			subdir :: string()}).

-record(enterdb_table, {name :: string(),
                        path :: string(),
                        key :: [atom()],
                        columns :: [atom()],
                        indexes :: [atom()],
			comparator :: ascending | descending,
                        type	:: atom(),
			options :: [table_option()],
                        shards :: []}).
%% enterdb shard tab
-record(enterdb_stab, {shard,
		       name,
		       type,
		       key,
		       columns,
		       indexes,
		       comparator,
		       data_model}).

-record(enterdb_ldb_resource, {name :: string(),
			       resource :: binary()
			      }).
