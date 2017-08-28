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


-module(enterdb_index_update).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

-export([get_pid/0]).

-export([index/5,
	 term_index_remove/4,
	 index_read/1]).

-export([register_ttl/3,
	 unregister_ttl/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(SERVER, ?MODULE).
-define(ADD, 43).
-define(REM, 45).
-define(TWO_BYTES, 2).
-define(FOUR_BYTES, 4).

-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb.hrl").
-include("enterdb_internal.hrl").

-type ixterm() :: unicode:charlist() |
                 {unicode:charlist(), integer()} |
                 {unicode:charlist(), integer(), integer()}.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get_pid() -> pid().
get_pid() ->
    whereis(?SERVER).

-spec index(DB :: binary(),
	    WriteOptions :: binary(),
	    TableId :: integer(),
	    Key :: binary(),
	    Terms :: [{integer(), ixterm()}]) ->
    ok.
index(DB, WriteOptions, TableId,
      Key, [{ColId, Terms} | Rest]) when is_integer(ColId)->
    Tid = encode_unsigned(?TWO_BYTES, TableId),
    Cid = encode_unsigned(?TWO_BYTES, ColId),
    term_index_update(?ADD, Tid, Cid, Key, Terms),
    index(DB, WriteOptions, TableId, Key, Rest);
index(DB, WriteOptions, TableId, Key, [_ | Rest]) ->
    index(DB, WriteOptions, TableId, Key, Rest);
index(_DB, _WriteOptions, _TableId, _Key, []) ->
    ok.

-spec term_index_remove(Tid :: binary(),
			Cid :: binary(),
			Key :: binary(),
			Terms :: string()) ->
    ok.
term_index_remove(_Tid, _Cid, _Key, undefined) ->
    ok;
term_index_remove(Tid, Cid, Key, Terms) ->
    term_index_update(?REM, Tid, Cid, Key, Terms).

-spec term_index_update(Op :: 43 | 45,
			Tid :: binary(),
			Cid :: binary(),
			Key :: binary(),
			Terms :: [ixterm()]) ->
    ok.
term_index_update(_Op, _Tid, _Cid, _Key, []) ->
    ok;
term_index_update(Op, Tid, Cid, Key, Terms) ->
    {ok, TidCid} = make_hash_key(Tid, Cid),
    {ok, Shard} = gb_hash:get_local_node(?TERM_INDEX_TABLE, TidCid),
    BinTerms = string_to_binary_terms(Terms),
    ?debug("ti req: ~p", [[Shard, TidCid, BinTerms, Op, Key]]),
    Res = (catch enterdb_rdb_worker:term_index(Shard, TidCid, BinTerms, encode_key(Op, Key))),
    ?debug("ti res: ~p", [Res]),
    Res.

-spec index_read(IxKey :: #{}) ->
    [{Stats :: binary(), Key :: binary()}].
index_read(IxKey) ->
    {ok, DBKey, DBHashKey} = make_index_key(IxKey),
    {ok, Shard} = gb_hash:get_local_node(?TERM_INDEX_TABLE, DBHashKey),
    Res = enterdb_rdb_worker:read(Shard, DBKey),
    parse_postings(Res).

-spec register_ttl(Name :: string(),
		   Tid :: integer(),
		   TTL :: integer()) ->
    ok.
register_ttl(?TERM_INDEX_TABLE, _, _) ->
    ok;
register_ttl(_, Tid, TTL) ->
    case enterdb_lib:get_tab_def(?TERM_INDEX_TABLE) of
        #{shards := Shards} ->
	    Req = {enterdb_rdb_worker, add_index_ttl, [Tid, TTL]},
	    enterdb_lib:map_shards(false, Req, Shards),
	    ok;
	_ ->
	    ok
    end.

-spec unregister_ttl(Name :: string(),
		     Tid :: integer()) ->
    ok.
unregister_ttl(?TERM_INDEX_TABLE, _) ->
    ok;
unregister_ttl(_, Tid) ->
    case enterdb_lib:get_tab_def(?TERM_INDEX_TABLE) of
        #{shards := Shards} ->
	    Req = {enterdb_rdb_worker, remove_index_ttl, [Tid]},
	    enterdb_lib:map_shards(false, Req, Shards),
	    ok;
	_ ->
	    ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    term_prep:init(),
    enterdb:create_table(?TERM_INDEX_TABLE,["tid", "cid", "term"],
			 [{type, rocksdb},
			  {comparator, ascending},
			  {hash_exclude, ["term"]},
			  {system_table, true},
			  {distributed, false}]),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({remove_term,
	    << Tid:?TWO_BYTES/bytes, Cid:?TWO_BYTES/bytes,
	    Key/binary >>, Terms}, State) ->
    ?debug("~p received remove_term: ~p -> ~p", [?SERVER, Terms]),
    spawn(?MODULE, term_index_remove, [Tid, Cid, Key, Terms]),
    {noreply, State};
handle_info(Info, State) ->
    ?debug("~p received unhandled info: ~p", [?SERVER, Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_hash_key(Tid, Cid) ->
    {ok, << Tid/binary, Cid/binary >>}.

make_index_key(#{tid := Tid, cid := Cid, term := Term}) ->
    TidBin = encode_unsigned(?TWO_BYTES, Tid),
    CidBin = encode_unsigned(?TWO_BYTES, Cid),
    TermBin = unicode:characters_to_binary(Term, unicode, utf8),
    HashKey = << TidBin/binary, CidBin/binary >>,
    {ok, << HashKey/binary, TermBin/binary >>, HashKey}.

encode_key(Op, Key) ->
    %%Length = Size of Key + 1 Byte (Op) + 4 Bytes (Ts)
    Length = encode_unsigned(?FOUR_BYTES, size(Key)+5),
    Ts = encode_unsigned(?FOUR_BYTES, erlang:system_time(second)),
    << Length/binary, <<Op>>/binary, Key/binary, Ts/binary >>.

encode_unsigned(Size, Int) when is_integer(Int) ->
    Unsigned = binary:encode_unsigned(Int, big),
    case Size - size(Unsigned) of
	Fill when Fill >= 0 ->
	    << <<0:Fill/unit:8>>/binary, Unsigned/binary >>;
	_ ->
	    Unsigned
    end.

parse_postings({ok, Binary}) ->
    parse_postings(Binary, []);
parse_postings({error, _Reason}) ->
    [].

parse_postings(<< Length:?FOUR_BYTES/big-unsigned-integer-unit:8, Bin/binary>>, Acc) ->
    %% Len = Length - 1 Byte (Op removed) - 4 Bytes (Ts)
    Len = Length-5,
    << Key:Len/bytes, _Ts:4/bytes, Stats:8/bytes, Rest/binary >> = Bin,
    Posting = {Stats, Key},
    parse_postings(Rest, [Posting | Acc]);
parse_postings(<<>>, Acc) ->
    lists:reverse(Acc).

string_to_binary_terms(Terms) ->
    ?debug("~p:~p(~p)",[?MODULE,string_to_binary_terms,Terms]),
    string_to_binary_terms(Terms, []).

string_to_binary_terms([{Str, F, P} | Rest], Acc) ->
    Bin = list_to_binary(Str),
    FreqBin = encode_unsigned(?FOUR_BYTES, F),
    PosBin = encode_unsigned(?FOUR_BYTES, P),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    FreqBin/binary,
				    PosBin/binary>> | Acc]);
string_to_binary_terms([{Str, F} | Rest], Acc) ->
    Bin = list_to_binary(Str),
    FreqBin = encode_unsigned(?FOUR_BYTES, F),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    FreqBin/binary,
				    <<0,0,0,0>>/binary>> | Acc]);
string_to_binary_terms([Str | Rest], Acc) when is_list(Str) ->
    Bin = list_to_binary(Str),
    string_to_binary_terms(Rest, [<<Bin/binary,
				    <<0,0,0,0,0,0,0,0>>/binary>> | Acc]);
string_to_binary_terms([], Acc)  ->
    lists:reverse(Acc).
