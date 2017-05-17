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
	 set_shard_ready_flag/2]).

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
    Pid = proc_lib:spawn(fun() -> do_start_recovery(Type, Node, Shard) end),
    ?info("started recovery process ~p for ~p (~p, ~p)", [Shard, Pid, Node, Type]),
    recovering.

do_start_recovery(log, Node, Shard) ->
    %% start local memlog while recovering
    ?info("start mem_log"),
    enterdb_shard_recovery:start({node(), Shard}, mem_log),
    ?info("set status recovering"),
    set_shard_ready_flag(Shard, recovering),
    {ok, Log} = rpc:call(Node, enterdb_shard_recovery, get_log, [{node(), Shard}]),
    ?info("got log ~p", [Log]),
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


update_local_shard(Node, Shard, need_full_recovery) ->
    copy_shard(Node, Shard);

update_local_shard(Node, Shard, Log) ->
    try
	R = rpc:call(Node, disk_log, chunk, [Log, start]),
	done = loop_through_data(Node, Log, R),
	ok = rpc:call(Node, enterdb_shard_recovery, stop, [{node(), Shard}])
   catch C:E ->
	?info("recover data failed ~p ~p", [{C,E}, erlang:get_stacktrace()]),
	?info("falling back to full recover of shard"),
	%% fallback to full recovery
	copy_shard(Node, Shard)
    end.

loop_through_data(_Node, _Log, eof) ->
    ?info("got eof"),
    done;
loop_through_data(Node, Log, {Cont, Data}) ->
    ?info("remote data ~p", [Data]),
    R = [apply(M, F, A) || {M,F,A} <- Data],
    ?info("applied log ~p", [R]),
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
    ?info("about to restore from ~p", [RestorePath]),
    Res = enterdb_rdb_worker:restore_db(Shard, 0, RestorePath),
    ?info("restore_db returned ~p", [Res]),
    %?info("deleting restore dir ~p", [RestorePath]),
    %os:cmd("rm -rf " ++ RestorePath),
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



%% start_receiver() ->
%%     {ok, LSocket} = gen_tcp:listen(0, [binary]),
%%     Report = self(),
%%     proc_lib:spawn_link(fun() -> {ok, Socket} = gen_tcp:accept(LSocket),
%% 				 receive_loop(Socket, #{report_to => Report, status => need_name, buf => <<>>}) end),
%%     {ok, _LocalPort} = inet:port(LSocket).
%%
%% receive_loop(_, State = #{rem_size := 0}) ->
%%     untar_restore(State),
%%     maps:get(report_to, State) ! ready_for_restore,
%%     ok;
%%
%% receive_loop(_, State = #{rem_size := Size}) when Size < 0 ->
%%     ?warning("this should not happen; remsize is negative"),
%%     untar_restore(State),
%%     maps:get(report_to, State) ! ready_for_restore,
%%     ok;
%%
%% receive_loop(Socket, State0) ->
%%     inet:setopts(Socket, [{active, once}]),
%%     Buf = maps:get(buf, State0),
%%     State = State0#{buf=><<>>},
%%     receive
%% 	{tcp, Socket, Data} ->
%% 	   NewState = handle_received_data(<<Buf/binary, Data/binary>>, State),
%% 	   gen_tcp:send(Socket, "ok"),
%% 	   receive_loop(Socket, NewState);
%% 	{Socket, Data} ->
%% 	    NewState = handle_received_data(<<Buf/binary, Data/binary>>, State),
%% 	    gen_tcp:send(Socket, "ok"),
%% 	    receive_loop(Socket, NewState);
%% 	M ->
%% 	    ?info("received unknonw message ~p", [M]),
%% 	    receive_loop(Socket, State)
%%     after 120000 ->
%% 	?warning("expected data.. stopping receiver"),
%% 	error_cleanup(State)
%%     end.
%%
%% handle_received_data(<<Size:32, Name0:Size/binary, Rest/binary>>, State = #{status := need_name}) ->
%%     Name = binary_to_list(Name0),
%%     {ok, Fd} = open_restore_file(Name),
%%     State#{status => need_size_data, fd=>Fd, name => Name, buf => Rest};
%%
%% handle_received_data(<<Size:64, Data/binary>>, State = #{status := need_size_data}) ->
%%     file:write(maps:get(fd, State), Data),
%%     State#{status => need_data, rem_size => Size - size(Data)};
%%
%% handle_received_data(Data, State = #{status := need_data, rem_size := Size}) ->
%%     file:write(maps:get(fd, State), Data),
%%     RemSize = Size - size(Data),
%%     State#{status => need_data, rem_size => RemSize};
%%
%% %% data not complete
%% handle_received_data(Data, State) ->
%%     State#{buf => Data}.
%%
%% open_restore_file(Name) ->
%%     RestoreDir = enterdb_lib:get_path("restore_path"),
%%     filelib:ensure_dir(RestoreDir ++ "/"),
%%     FilePath = filename:join([RestoreDir, Name]),
%%     ?info("creating restore file ~p", [FilePath]),
%%     file:open(FilePath, [write]).
%%
%% error_cleanup(_State = #{name := Name, fd := Fd}) ->
%%     file:close(Fd),
%%     RestoreDir = enterdb_lib:get_path("restore_path"),
%%     file:delete(RestoreDir ++ Name).
%%
%% %% TODO: stream tar and untar while receiveing data
%% untar_restore(_State = #{name := Name, fd := Fd}) ->
%%     file:close(Fd),
%%     RestoreDir = enterdb_lib:get_path("restore_path"),
%%     FilePath = filename:join([RestoreDir, Name]),
%%     Cmd = "tar -C " ++ RestoreDir ++" -xzf " ++ FilePath,
%%     ?info("about to untar received backup with cmd ~p", [Cmd]),
%%     os:cmd(Cmd),
%%     ?debug("cleaning up received tar ~p", [FilePath]),
%%     file:delete(FilePath),
%%     ok.
%%
%%
%% send_backup(Shard, Hostname, Port) ->
%%     BackupPath = enterdb_lib:get_path("backup_path"),
%%     Name = maps:get(name, enterdb_lib:get_shard_def(Shard)),
%%     BackupDir = filename:join([BackupPath, Name]),
%%     FilePath = filename:join([BackupDir, Shard ++ ".tar.gz"]),
%%     %% TODO: stream tar and untar while receiveing data; maybe untar directly over the shard?
%%     Cmd = "tar -C " ++ BackupDir ++" -czf " ++ FilePath ++ " " ++ Shard,
%%     ?info("about to tar backup before sending with cmd ~p", [Cmd]),
%%     OSRes = os:cmd(Cmd),
%%     ?info("send backup to ~p:~p", [Hostname, Port]),
%%     start_send(FilePath, Hostname, Port).
%%
%% start_send(FilePath, Hostname, Port) ->
%%     {ok, Socket} = gen_tcp:connect(Hostname, Port, []),
%%     ?info("connected"),
%%     FileSize = filelib:file_size(FilePath),
%%     BaseName = list_to_binary(filename:basename(FilePath)),
%%     gen_tcp:send(Socket, <<(size(BaseName)):32, BaseName/binary, FileSize:64>>),
%%     {ok, Fd} = file:open(FilePath, [read, binary]),
%%     loop_send_file(Fd, Socket).
%% loop_send_file(Fd, Socket) ->
%%     case file:read(Fd, 1024 * 1024) of
%% 	{ok, Data} ->
%% 	    gen_tcp:send(Socket, Data),
%% 	    loop_send_file(Fd, Socket);
%% 	eof ->
%% 	    %% wait for other end to receive data
%% 	    close_loop(Socket)
%%     end.
%%
%% close_loop(Socket) ->
%%     receive
%% 	{tcp, Socket, Data} ->
%% 	    ?info("received ~p", [Data]),
%% 	    close_loop(Socket);
%% 	{Socket, Data} ->
%% 	    ?info("received ~p", [Data]),
%% 	    close_loop(Socket);
%% 	{tcp_closed, Socket} ->
%% 	    ?info("tcp closed");
%% 	M ->
%% 	    ?info("unknown message closing: ~p", [M]),
%% 	    done
%%     after 40000 ->
%% 	?info("timeout"),
%% 	{error, timeout}
%%     end.

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
