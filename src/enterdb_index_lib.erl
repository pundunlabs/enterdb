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

-export([make_lookup_terms/2,
	 read/4]).

-include("enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================
-spec make_lookup_terms(IndexOptions :: map(),
			Term :: string()) ->
    [unicode:charlist()].
make_lookup_terms(IndexOptions, Term) ->
    TokenFilter = maps:get(token_filter, IndexOptions, #{}),
    ReadTokenFilter = maps:merge(TokenFilter, #{add => [], stats => unique}),
    ReadOptions = maps:put(token_filter, ReadTokenFilter, IndexOptions),
    term_prep:analyze(ReadOptions, Term).

-spec read(TD :: #{},
	   Cid :: binary(),
	   Terms :: [unicode:charlist()],
	   Filter :: posting_filter()) ->
    {ok, [posting()]} | {error, Reason :: term()}.
read(#{key := KeyDef,
       distributed := Dist,
       shards := Shards}, Cid, [Term], Filter) ->
    RawPostingsList = read_term(Cid, Term, Dist, Shards),
    {ok, filter(KeyDef, RawPostingsList, Filter)};
read(#{key := KeyDef,
       distributed := Dist,
       shards := Shards}, Cid, Terms, Filter) ->
    RawPostingsList = pl_intersection(Terms, Cid, Dist, Shards),
    {ok, filter(KeyDef, RawPostingsList, Filter)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

read_term(Cid, Term, Dist, Shards) ->
    {ok, DBKey} = make_index_key(Cid, Term),
    Req = {enterdb_rdb_worker, index_read, [DBKey]},
    ResL = enterdb_lib:map_shards_seq(Dist, Req, Shards),
    AllPostings = [parse_postings(R) || R <- ResL],
    {ok, RawPostingsList} = enterdb_utils:merge_sorted_kvls(0, AllPostings),
    RawPostingsList.

pl_intersection(Terms, Cid, Dist, Shards)->
    Count = length(Terms),
    Map = pl_union(Terms, Cid, Dist, Shards, #{}),
    maps:fold(fun(K, V, Acc) when length(V) == Count -> [{least(V), K} | Acc];
		 (_, _, Acc) -> Acc
	      end, [], Map).

pl_union([Term | Rest], Cid, Dist, Shards, Acc) ->
    PostingList = read_term(Cid, Term, Dist, Shards),
    pl_union(Rest, Cid, Dist, Shards, pl_update(PostingList, Acc));
pl_union([], _Cid, _Dist, _Shards, Acc) ->
    Acc.

pl_update([{S, P} | Rest], Acc) ->
    pl_update(Rest, maps:update_with(P, fun(V) -> [S | V] end, [S], Acc));
pl_update([], Acc) ->
    Acc.

-spec filter(Keydef :: [term()] | used,
	     List :: [binary()] | [map()],
	     Filter :: posting_filter()) ->
    {ok, [posting()]}.
filter(KeyDef, List, #{sort_by := timestamp} = Filter) ->
    PostingsList = make_postings_list(KeyDef, List),
    Acc = lists:sort(fun (#{ts := A}, #{ts := B}) -> A >= B end, PostingsList),
    filter(used, Acc, maps:remove(sort_by, Filter));
filter(KeyDef, List, #{start_ts := Ts} = Filter) ->
    PostingsList = make_postings_list(KeyDef, List),
    Acc = lists:filter(fun (#{ts := A}) -> A >= Ts end, PostingsList),
    filter(used, Acc, maps:remove(start_ts, Filter));
filter(KeyDef, List, #{end_ts := Ts} = Filter) ->
    PostingsList = make_postings_list(KeyDef, List),
    Acc = lists:filter(fun (#{ts := A}) -> Ts >= A end, PostingsList),
    filter(used, Acc, maps:remove(end_ts, Filter));
filter(KeyDef, List, #{max_postings := Max} = Filter) ->
    Acc = sublist(List, Max),
    filter(KeyDef, Acc, maps:remove(max_postings, Filter));
filter(KeyDef, List, _Filter) ->
    make_postings_list(KeyDef, List).

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_index_key(Cid, Term) ->
    CidBin = enterdb_lib:encode_unsigned(2, Cid),
    TermBin = unicode:characters_to_binary(Term, unicode, utf8),
    {ok, << CidBin/binary, TermBin/binary >>}.

parse_postings({ok, Binary}) ->
    parse_postings(Binary, []);
parse_postings(_E) ->
    ?debug("index read got: ~p",[_E]),
    [].

parse_postings(<< Length:4/little-unsigned-integer-unit:8, Bin/binary>>, Acc) ->
    %% Len = Length - 4 Bytes (Length) - 12 Bytes (Stats)
    Len = Length-16,
    << Key:Len/bytes, Stats:12/bytes, Rest/binary >> = Bin,
    Posting = {Stats, Key},
    parse_postings(Rest, [Posting | Acc]);
parse_postings(<<>>, Acc) ->
    lists:reverse(Acc).

sublist(List, Int) when is_integer(Int), Int > 0 ->
    lists:sublist(List, Int);
sublist(List, _) ->
    List.

make_postings_list(used, Postings) ->
    Postings;
make_postings_list(KeyDef, Postings) ->
    [make_post(KeyDef, S, B) || {S, B} <- Postings].

make_post(KeyDef, BinStats, BinKey) ->
    Key = enterdb_lib:make_app_key(KeyDef, BinKey),
    maps:put(key, Key, make_stats(BinStats)).

make_stats(<<Freq:4/big-unsigned-integer-unit:8,
	     Pos:4/big-unsigned-integer-unit:8,
	     Ts:4/little-unsigned-integer-unit:8>>) ->
    #{freq => Freq, pos => Pos, ts => Ts}.

least([<<Freq:4/unit:8, Pos:4/unit:8, Ts:4/unit:8>> | Rest]) ->
    least(Rest, Freq, Pos, Ts).

least([<<F:4/unit:8, P:4/unit:8, _/binary>> | Rest], Freq, Pos, Ts) ->
    least(Rest, min(F, Freq), min(P, Pos), Ts);
least([], Freq, Pos, Ts) ->
    <<Freq:4/unit:8, Pos:4/unit:8, Ts:4/unit:8>>.
