%%%===================================================================
%% @author Jonas Falkevik
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
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================
-module(enterdb_recovery).

-include_lib("gb_log/include/gb_log.hrl").
-include("enterdb_internal.hrl").
-include("enterdb.hrl").

-export([check_ready_status/1,
	 log_event/2,
	 log_event/3,
	 set_shard_ready_flag/2,
	 start_recovery/3]).

-export([do_backup/2]).

-export([get_metadata_for_shard/1]).

-spec log_event(Node :: atom(), Event :: term()) ->
    ok | {error, Reason :: term()}.
log_event(Node, {_, _, [Shard|_]} = Event) when Node /= node() ->
    log_event(Node, Shard, Event);

%% don't log event for local node
%% should be fixed so that we are not getting here
log_event(_, _) ->
    ok.

-spec log_event(Node :: atom(), Shard :: string(), Event :: term()) ->
    ok | {error, Reason :: term()}.
log_event(Node, Shard, {M,do_write,A}) ->
    Event = {M,do_write_force,A},
    enterdb_shard_recovery:log_event({Node,Shard}, Event, start),
    ok;
log_event(Node, Shard, {M,do_update,A}) ->
    Event = {M,do_update_force,A},
    enterdb_shard_recovery:log_event({Node,Shard}, Event, start),
    ok;
log_event(Node, Shard, {M,do_delete,A}) ->
    Event = {M,do_delete_force,A},
    enterdb_shard_recovery:log_event({Node,Shard}, Event, start),
    ok;
log_event(_Node, _Shard, _Event) ->
    ok.

check_ready_status(#{shard := Shard}) ->
    ShardMetaData = get_metadata_for_shard(Shard),
    _ReadyStatus = do_we_need_recovery(ShardMetaData, Shard).

get_metadata_for_shard(Shard) ->
    case gb_dyno_metadata:lookup_topo() of
	{ok, Data} ->
	    Nodes = proplists:get_value(nodes, Data),
	    NodeMetaData = proplists:get_value(node(), Nodes),
	    _ShardInfo = proplists:get_value({Shard, oos}, NodeMetaData, ready);
	_ ->
	    ?warning("could not get metadata"),
	    {error, no_metadata}
    end.

do_we_need_recovery(ready, Shard) ->
    set_shard_ready_flag(Shard, ready),
    ready;
do_we_need_recovery({log, Node}, Shard) ->
    start_recovery(log, Node, Shard);
do_we_need_recovery({full, Node}, Shard) ->
    start_recovery(full, Node, Shard).

start_recovery(Type, Node, Shard) ->
    Pid = proc_lib:spawn(fun() -> init_start_recovery(Type, Node, Shard) end),
    ?info("started recovery process ~p for ~p (~p, ~p)", [Shard, Pid, Node, Type]),
    recovering.

init_start_recovery(Type, Node, Shard) ->
    case enterdb_recovery_srv:reg(Shard, self()) of
	ok ->
	    do_start_recovery(Type, Node, Shard);
	{error, already_running} ->
	    ?info("recovery already running");
	E ->
	    ?info("could not register recovery process: ~p", [E])
    end.

do_start_recovery(log, Node, Shard) ->
    %% start local memlog while recovering
    ?info("start mem_log"),
    enterdb_shard_recovery:start({node(), Shard}, mem_log),
    ?info("set status recovering"),
    set_shard_ready_flag(Shard, recovering),
    Log = rpc:call(Node, enterdb_shard_recovery, get_log, [{node(), Shard}]),
    ?info("remote log ~p", [Log]),
    update_local_shard(Node, Shard, Log),
    update_column_mapper(Node, Shard),
    enterdb_shard_recovery:dump_mem_log({node(), Shard}),
    set_shard_ready_flag(Shard, recoverd),
    enterdb_shard_recovery:dump_mem_log({node(), Shard}),
    set_shard_ready_flag(Shard, ready),
    gb_dyno_metadata:node_rem_prop(node(), {Shard, oos}),
    ?info("recovery done for shard ~p", [Shard]);

do_start_recovery(full, Node, Shard) ->
    %% start local memlog while recovering
    enterdb_shard_recovery:start({node(), Shard}, mem_log),
    rpc:call(Node, enterdb_shard_recovery, stop, [{node(), Shard}]),
    set_shard_ready_flag(Shard, recovering),
    copy_shard(Node, Shard),
    update_column_mapper(Node, Shard),
    enterdb_shard_recovery:dump_mem_log({node(), Shard}),
    set_shard_ready_flag(Shard, recoverd),
    enterdb_shard_recovery:dump_mem_log({node(), Shard}),
    set_shard_ready_flag(Shard, ready),
    %% remove metadata about shard
    gb_dyno_metadata:node_rem_prop(node(), {Shard, oos}),
    ?info("recovery done for shard ~p", [Shard]).

update_local_shard(Node, Shard, {ok, Log}) ->
    try
	R = rpc:call(Node, disk_log, chunk, [Log, start]),
	done = loop_through_data(Node, Log, R),
	ok = rpc:call(Node, enterdb_shard_recovery, stop, [{node(), Shard}])
   catch C:E:ST ->
	?warning("recover data failed ~p ~p", [{C,E}, ST]),
	?warning("falling back to full recover of shard"),
	%% fallback to full recovery
	copy_shard(Node, Shard)
    end;
update_local_shard(Node, Shard, _RemoteLogStatus) ->
    copy_shard(Node, Shard).

loop_through_data(_Node, _Log, eof) ->
    done;
loop_through_data(Node, Log, {Cont, Data}) ->
    _R = [apply(M, F, A) || {M,F,A} <- Data],
    loop_through_data(Node, Log, rpc:call(Node, disk_log, chunk, [Log, Cont])).

update_column_mapper(Node, Shard) ->
    STAB = enterdb_lib:get_shard_def(Shard),
    CM = maps:get(column_mapper, STAB),
    RemoteDefs = rpc:call(Node,gb_reg, all, [CM]),
    gb_reg:add(CM, maps:to_list(RemoteDefs)).


copy_shard(Node, Shard) ->
    {ok, RemPath} = rpc:call(Node, ?MODULE, do_backup, [Shard, 20 *60 * 1000]),
    ?info("Backup on ~p for ~p in ~p", [Node, Shard, RemPath]),

    RestoreDir = enterdb_lib:get_path("restore_path"),
    RestorePath = filename:join([RestoreDir, Shard]),
    ?info("cleaning out any lingering data in ~p", [RestorePath]),
    ok = copy_backup(Node, Shard, RestorePath, RemPath),
    ?info("about to restore ~p from ~p", [Shard, RestorePath]),
    Res = enterdb_rdb_worker:restore_db(Shard, 0, RestorePath),
    ?info("restore_db ~p returned ~p", [Shard, Res]),
    case Res of
	{error, no_ns_entry} ->
	    enterdb_lib:recreate_shard(Shard),
	    OpenRes = enterdb_lib:open_shard(Shard),
	    ?info("open shard ~p returned ~p", [Shard, OpenRes]),
	    ?info("trying to restore db again"),
	    ok = enterdb_rdb_worker:restore_db(Shard, 0, RestorePath);
	_ -> ok
    end,
    ?info("deleting restore dir ~p", [RestorePath]),
    os:cmd("rm -rf " ++ RestorePath),
    ok.

do_backup(Shard, Timeout) ->
    RestoreDir = enterdb_lib:get_path("restore_path"),
    RestorePath = filename:join([RestoreDir, Shard]),
    ?info("cleaning out any lingering data in ~p", [RestorePath]),
    os:cmd("rm -rf " ++ RestorePath),
    filelib:ensure_dir(RestorePath ++ "/"),
    Res = enterdb_rdb_worker:backup_db(Shard, RestorePath, Timeout),
    {Res, RestorePath}.

copy_backup(Node, _Shard, RestorePath, RemotePath) ->
    filelib:ensure_dir(RestorePath ++ "/"),
    {ok, Pid, Port} = gb_stream_tar:start_receiver(self(), RestorePath),
    {ok, Hostname} = inet:gethostname(),
    R = rpc:call(Node, gb_stream_tar, start_sender, [RemotePath, Hostname, Port]),
    ?info("send_backup returned ~p", [R]),
    receive
	{Pid, done} ->
	       ok;
	{error, R} ->
	    {error, R}
    after
	10*60000 ->
	    {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Set the #enterdb_stab ready flag
%% @end
%%--------------------------------------------------------------------
-spec set_shard_ready_flag(Shard :: shard_name(),
			   Flag  :: ready | not_ready) ->
    ok | {error, Reason :: term()}.
set_shard_ready_flag(Shard, Flag) ->
    Fun =
	fun() ->
	    [S = #enterdb_stab{map=Map}] = mnesia:read(enterdb_stab, Shard),
	    NewMap = Map#{ready_status=>Flag},
	    mnesia:write(S#enterdb_stab{map = NewMap})
	end,
    case enterdb_db:transaction(Fun) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
           {error, {aborted, Reason}}
    end.
