%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2017 Pundun Labs AB
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
%% @doc
%% Module Description:
%% @end
%%%===================================================================


-module(enterdb_oam).

%% Application callbacks
-export([backup_table/1,
         create_checkpoint/1]).

%%%===================================================================
%%% API
%%%===================================================================
-spec backup_table(TableName :: string()) ->
    ok | {error, Reason :: term()}.
backup_table(_) ->
    ok.

-spec create_checkpoint(TableName :: string()) ->
    ok | {error, Reason :: term()}.
create_checkpoint(_) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================
