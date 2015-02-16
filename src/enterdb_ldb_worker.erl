%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%%
%%% @end
%%% Created : 16 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(enterdb_ldb_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {db_ref,
                name,
                options,
                path,
                hash}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    Name = list_to_atom(proplists:get_value(name, Args)),
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

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
init(Args) ->
    Name = proplists:get_value(name, Args),
    Hash = proplists:get_value(hash, Args),
    OptionsPL = proplists:get_value(options, Args),
    Path = proplists:get_value(path, Args),
    
    OptionsReq = build_leveldb_options(OptionsPL),

    case leveldb:options(OptionsReq) of
        {ok, Options} ->
            FullPath = filename:join(Path, Name),
            case leveldb:open_db(Options, FullPath) of
                {error, Reason} ->
                    {stop, {error, Reason}};
                {ok, DB} ->
                    {ok, #state{db_ref = DB,
                                name = Name,
                                options = OptionsReq,
                                path = Path,
                                hash = Hash}}
            end;
        {error, Reason} ->
            {stop, {error, Reason}}
    end. 
                                   
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
handle_info(_Info, State) ->
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

%%--------------------------------------------------------------------
%% @doc
%% Build a #leveldb_options{} record with provided proplist.
%%
%%--------------------------------------------------------------------
-spec build_leveldb_options(OptionsPL::[{atom(), term()}]) -> ok | {error, Reason::term()}.
build_leveldb_options(OptionsPL) ->
    leveldb_lib:build_leveldb_options(OptionsPL).
