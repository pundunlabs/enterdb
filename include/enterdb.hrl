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
-type wrapper() :: {file_margin(), time_margin()}.

-type table_option() :: [{time_ordered, boolean()} |
                         {wrapped, wrapper()} |
			 {backend, backend()} |
                         {data_model, data_model()}].

-record(enterdb_shard, {hash :: binary(),
                        name :: atom()
                       }).

-record(enterdb_table, {name :: string(),
                        path :: string(),
                        key :: [atom()],
                        columns :: [atom()],
                        indexes :: [atom()],
                        options :: [table_option()],
                        shards :: [string()]
                       }).

-record(enterdb_ldb_resource, {name :: atom(),
			       resource :: binary()
			      }).