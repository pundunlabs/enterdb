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

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(SERVER, ?MODULE).

-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb.hrl").
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
index(DB, WriteOptions, TableId, Key, [{ColId, Term} | Rest]) ->
    io:format("~p:~p, Args: ~p~n",[?MODULE,?LINE, [DB, WriteOptions, TableId, Key, [{ColId, Term} | Rest]]]),
    Tid = <<TableId:16>>,
    Cid = <<ColId:16>>,
    IndexKey = << Tid/binary, Cid/binary, Key/binary >>,
    ok = rocksdb:index_merge(DB, WriteOptions, IndexKey, Term),
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
    AddTermIndexKey = [{"tid", Tid}, {"cid", Cid}, {"term", NewTerm}],
    RemTermIndexKey = [{"tid", Tid}, {"cid", Cid}, {"term", OldTerm}],
    #{key := KeyDef, hash_key := HashKey} =
	enterdb_lib:get_tab_def(?TERM_INDEX_TABLE),
    {ok, ADBKey, ADBHashKey} = enterdb_lib:make_db_key(KeyDef, HashKey, AddTermIndexKey),
    {ok, {AShard, ARing}} = gb_hash:get_node(?TERM_INDEX_TABLE, ADBHashKey),
    {ok, RDBKey, RDBHashKey} = enterdb_lib:make_db_key(KeyDef, HashKey, RemTermIndexKey),
    {ok, {RShard, RRing}} = gb_hash:get_node(?TERM_INDEX_TABLE, RDBHashKey),
    ?dyno:call(ARing, {enterdb_rdb_worker, term_index,
			[AShard, ADBKey, encode_key(43, Key)]}, write),
    ?dyno:call(RRing, {enterdb_rdb_worker, term_index,
			[RShard, RDBKey, encode_key(45, Key)]}, write).

-spec index_read(KeyDef :: key(),
		 Key :: [{Tag :: string(), NewTerm :: integer() | string()}]) ->
    {ok, [term()]}.
index_read(KeyDef, Key) ->
    #{key := TermKeyDef, hash_key := HashKey} =
	enterdb_lib:get_tab_def(?TERM_INDEX_TABLE),
    {ok, DBKey, DBHashKey} = enterdb_lib:make_db_key(TermKeyDef, HashKey, Key),
    {ok, {Shard, Ring}} = gb_hash:get_node(?TERM_INDEX_TABLE, DBHashKey),
    Res = ?dyno:call(Ring, {enterdb_rdb_worker, read,[Shard, DBKey]}, read),
    parse_postings(KeyDef, Res).


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
			 [{type, rocksdb}, {hash_exclude, ["term"]}]),
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
    io:format("~p:~p, received same term: ~p~n",[?MODULE,?LINE, Term]),
    {noreply, State};
handle_info({index_update, << Tid:16, Cid:16, Key/binary >>,
	     NewTerm, OldTerm}, State) ->
    ?debug("~p received term change: ~p -> ~p", [?SERVER, OldTerm, NewTerm]),
    io:format("~p:~p, received term change: ~p -> ~p~n",[?MODULE,?LINE, OldTerm, NewTerm]),
    spawn(?MODULE, term_index_update, [Tid, Cid, Key, NewTerm, OldTerm]),
    {noreply, State};
handle_info(Info, State) ->
    ?debug("~p received unhandled info: ~p", [?SERVER, Info]),
    io:format("~p:~p, received unhandled info: ~p~n",[?MODULE,?LINE, Info]),
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
encode_key(Op, Key) ->
    Unsigned = binary:encode_unsigned(size(Key)+1),
    Length =
	case 4 - size(Unsigned) of
	    3 -> << <<0,0,0>>/binary, Unsigned/binary >>;
	    2 -> << <<0,0>>/binary, Unsigned/binary >>;
	    1 -> << <<0>>/binary, Unsigned/binary >>;
	    0 -> Unsigned
    end,
    << Length/binary, <<Op>>/binary, Key/binary >>.

parse_postings(KeyDef, {ok, Binary}) ->
    parse_postings(KeyDef, Binary, []);
parse_postings(_, Else) ->
    Else.

parse_postings(KeyDef, << Length:4/big-unsigned-integer-unit:8, Bin/binary>>, Acc) ->
    Len = (Length-1),
    << Key:Len/bytes, Rest/binary >> = Bin,
    parse_postings(KeyDef, Rest, [enterdb_lib:make_app_key(KeyDef, Key) | Acc]);
parse_postings(_, <<>>, Acc) ->
    Acc.
