%% Enterdb records and macro definitions.

-define(MAX_TABLE_NAME_LENGTH, 64).

-type backend() :: leveldb | ets_leveldb.
-type data_model() :: binary | array | hash.
-type table_option() :: [{time_ordered, boolean()} |
                         {backend, backend()} |
                         {data_model, data_model()}].

-record(enterdb_shard, {hash :: binary(),
                        name :: atom()
                       }).

-record(enterdb_table, {name :: atom(),
                        path :: string(),
                        key :: [atom()],
                        columns :: [atom()],
                        indexes :: [atom()],
                        options :: [table_option()],
                        shards :: [string()]
                        }).
