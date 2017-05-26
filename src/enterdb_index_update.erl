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
	 term_index_update/5,
	 index_read/2]).

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
	    Terms :: [{integer(), string()}]) ->
    ok.
index(DB, WriteOptions, TableId,
      Key, [{ColId, Term} | Rest]) when is_integer(ColId)->
    Tid = encode_unsigned(?TWO_BYTES, TableId),
    Cid = encode_unsigned(?TWO_BYTES, ColId),
    IndexKey = << Tid/binary, Cid/binary, Key/binary >>,
    ok = rocksdb:index_merge(DB, WriteOptions, IndexKey, list_to_binary(Term)),
    index(DB, WriteOptions, TableId, Key, Rest);
index(DB, WriteOptions, TableId, Key, [_ | Rest]) ->
    index(DB, WriteOptions, TableId, Key, Rest);
index(_DB, _WriteOptions, _TableId, _Key, []) ->
    ok.

-spec term_index_update(Tid :: integer(),
			Cid :: integer(),
			Key :: binary(),
			NewTerm :: string(),
			OldTerm :: string()) ->
    ok.
term_index_update(Tid, Cid, Key, NewTerm, OldTerm) ->
    term_index_update_(?ADD, Tid, Cid, Key, NewTerm),
    term_index_update_(?REM, Tid, Cid, Key, OldTerm).

term_index_update_(_, _, _, _, undefined) ->
    ok;
term_index_update_(Op, Tid, Cid, Key, Terms) ->
    Tokens = string:tokens(Terms, " "),
    [term_index_update__(Op, Tid, Cid, Key, T) || T <- Tokens].

term_index_update__(Op, Tid, Cid, Key, Term) ->
    {ok, DBKey, DBHashKey} = make_index_key(Tid, Cid, Term),
    {ok, Shard} = gb_hash:get_local_node(?TERM_INDEX_TABLE, DBHashKey),
    enterdb_rdb_worker:term_index(Shard, DBKey, encode_key(Op, Key)).

-spec index_read(KeyDef :: key(),
		 IxKey :: #{}) ->
    {ok, [term()]}.
index_read(KeyDef, IxKey) ->
    {ok, DBKey, DBHashKey} = make_index_key(IxKey),
    {ok, Shard} = gb_hash:get_local_node(?TERM_INDEX_TABLE, DBHashKey),
    Res = enterdb_rdb_worker:read(Shard, DBKey),
    parse_postings(KeyDef, Res).

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
handle_info({index_update, _, Term, Term}, State) ->
    ?debug("~p received same term: ~p", [?SERVER, Term]),
    %%io:format("~p:~p, received same term: ~p~n", [?MODULE, ?LINE, Term]),
    {noreply, State};
handle_info({index_update,
	    << Tid:?TWO_BYTES/big-unsigned-integer-unit:8,
	    Cid:?TWO_BYTES/big-unsigned-integer-unit:8,
	    Key/binary >>, NewTerm, OldTerm}, State) ->
    ?debug("~p received term change: ~p -> ~p", [?SERVER, OldTerm, NewTerm]),
    %%io:format("~p:~p, received term change: ~s -> ~s~n", [?MODULE, ?LINE, OldTerm, NewTerm]),
    spawn(?MODULE, term_index_update, [Tid, Cid, Key, NewTerm, OldTerm]),
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
make_index_key(#{tid := Tid, cid := Cid, term := Term}) ->
    make_index_key(Tid, Cid, Term).

make_index_key(Tid, Cid, Term) ->
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

parse_postings(KeyDef, {ok, Binary}) ->
    parse_postings(KeyDef, Binary, []);
parse_postings(_, {error, _Reason}) ->
    [].

parse_postings(KeyDef, << Length:?FOUR_BYTES/big-unsigned-integer-unit:8, Bin/binary>>, Acc) ->
    %% Len = Length - 1 Byte (Op removed) - 4 Bytes (Ts)
    Len = Length-5,
    << Key:Len/bytes, _Ts:4/bytes, Rest/binary >> = Bin,
    parse_postings(KeyDef, Rest, [enterdb_lib:make_app_key(KeyDef, Key) | Acc]);
parse_postings(_, <<>>, Acc) ->
    Acc.
