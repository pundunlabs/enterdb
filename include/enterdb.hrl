%% Enterdb records and macro definitions.

-define(MAX_TABLE_NAME_LENGTH, 64).

-type key() :: [{atom(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {atom(), term()}.

-type type() :: leveldb |
		ets_leveldb |
		leveldb_wrapped |
		ets_leveldb_wrapped.

-type data_model() :: binary | array | hash.

-type time_margin() :: {seconds, pos_integer()} |
		       {minutes, pos_integer()} |
		       {hours, pos_integer()} |
		       undefined.

-type size_margin() :: {bytes, pos_integer()} |
		       undefined.

-type comparator() :: ascending | descending.

-record(enterdb_wrapper, {bucket_margin :: pos_integer(),
			  time_margin :: time_margin(),
			  size_margin :: size_margin()
			 }).

-type table_option() :: [{time_ordered, boolean()} |
                         {wrapped, #enterdb_wrapper{}} |
			 {mem_wrapped, #enterdb_wrapper{}} |
			 {type, type()} |
                         {data_model, data_model()} |
			 {comparator, comparator()} |
			 {shards, integer()} |
			 {nodes, [atom()]}].

-type timestamp() :: {pos_integer(),  %% mega seconds &
		      pos_integer(),  %% seconds &
		      pos_integer()}. %% micro seconds since start of epoch(UNIX)

-type op() :: first | last | {seek, key()} | next | prev.
-type it() :: binary().

-record(enterdb_shard, {name :: string(),
			subdir :: string()}).

-type node_name() :: atom().
-type shard_name() :: string().

-record(enterdb_table, {name :: string(),
                        path :: string(),
                        key :: [atom()],
                        columns :: [atom()],
                        indexes :: [atom()],
			comparator :: comparator(),
                        type	:: type(),
			data_model :: data_model(),
			options :: [table_option()],
                        shards :: [{node_name(), shard_name()}]}).
%% enterdb shard tab
-record(enterdb_stab, {shard :: shard_name(),
		       name :: string(),
		       type :: type(),
		       key :: [atom()],
		       columns :: [atom()],
		       indexes :: [atom()],
		       comparator :: comparator(),
		       data_model :: data_model()}).

-record(enterdb_ldb_resource, {name :: shard_name(),
			       resource :: binary()
			      }).

-record(enterdb_lit_resource, {name :: shard_name(),
			       pid :: pid(),
			       it :: binary()
			     }).
