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


-module(enterdb_index_lib).

-export([read/3]).

-define(TWO_BYTES, 2).
-define(FOUR_BYTES, 4).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================

-spec read(TD :: #{},
	   IxKey :: #{},
	   Limit :: pos_integer() | undefined) ->
    {ok, [key()]} | {error, Reason :: term()}.
read(#{key := KeyDef,
       distributed := Dist,
       shards := Shards}, IxKey, Limit) ->
    {ok, DBKey} = make_index_key(IxKey),
    Req = {enterdb_rdb_worker, index_read, [DBKey]},
    ResL = enterdb_lib:map_shards(Dist, Req, Shards),
    AllPostings = [parse_postings(R) || R <- ResL],
    {ok, Postings} = enterdb_utils:merge_sorted_kvls(0, AllPostings),
    Sublist = sublist(Postings, Limit),
    {ok, [enterdb_lib:make_app_key(KeyDef, B) || {_, B} <- Sublist]}.
    

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_index_key(#{cid := Cid, term := Term}) ->
    CidBin = enterdb_lib:encode_unsigned(?TWO_BYTES, Cid),
    TermBin = unicode:characters_to_binary(Term, unicode, utf8),
    {ok, << CidBin/binary, TermBin/binary >>}.

parse_postings({ok, Binary}) ->
    parse_postings(Binary, []);
parse_postings({error, _Reason}) ->
    [].

parse_postings(<< Length:?FOUR_BYTES/little-unsigned-integer-unit:8, Bin/binary>>, Acc) ->
    %% Len = Length - 4 Bytes (Length) - 8 Bytes (Stats) - 4 Bytes (Ts)
    Len = Length-16,
    << Key:Len/bytes, Stats:8/bytes, _Ts:4/bytes, Rest/binary >> = Bin,
    Posting = {Stats, Key},
    parse_postings(Rest, [Posting | Acc]);
parse_postings(<<>>, Acc) ->
    lists:reverse(Acc).

sublist(List, Int) when is_integer(Int), Int > 0 ->
    lists:sublist(List, Int);
sublist(List, _) ->
    List.
