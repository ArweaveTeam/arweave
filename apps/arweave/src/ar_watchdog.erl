%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc Watchdog process. Logs the information about mined blocks or missing external blocks.
-module(ar_watchdog).

-behaviour(gen_server).

-export([
	start_link/0,
	started_hashing/0,
	block_received_n_confirmations/2,
	mined_block/2,
	foreign_block/1
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	mined_blocks,
	no_foreign_blocks_timer,
	miner_logging = false
}).

%%%===================================================================
%%% API
%%%===================================================================

started_hashing() ->
	gen_server:cast(?MODULE, started_hashing).

block_received_n_confirmations(BH, Height) ->
	gen_server:cast(?MODULE, {block_received_n_confirmations, BH, Height}).

mined_block(BH, Height) ->
	gen_server:cast(?MODULE, {mined_block, BH, Height}).

foreign_block(BH) ->
	gen_server:cast(?MODULE, {foreign_block, BH}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	MinerLogging = not lists:member(miner_logging, Config#config.disable),
	{ok, Timer} = timer:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), no_foreign_blocks),
	State = #state{
		mined_blocks = maps:new(),
		miner_logging = MinerLogging,
		no_foreign_blocks_timer = Timer
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
	?LOG_ERROR([{event, unhandled_call}, {request, Request}]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(started_hashing, State) when State#state.miner_logging == true ->
	Message = "[Stage 1/3] Starting to hash",
	?LOG_INFO(Message),
	ar:console("~s~n", [Message]),
	{noreply, State};

handle_cast(started_hashing, State) ->
	{noreply, State};

handle_cast({block_received_n_confirmations, BH, Height}, State) ->
	MinedBlocks = State#state.mined_blocks,
	UpdatedMinedBlocks = case maps:take(Height, MinedBlocks) of
		{BH, Map} when State#state.miner_logging == true ->
			%% Log the message for block mined by the local node
			%% got confirmed by the network.
			Message =
				io_lib:format(
					"[Stage 3/3] Your block ~s was accepted by the network!",
					[ar_util:encode(BH)]
				),
			?LOG_INFO(Message),
			ar:console("~s~n", [Message]),
			Map;
		{_, Map} ->
			Map;
		error ->
			MinedBlocks
	end,
	{noreply, State#state{ mined_blocks = UpdatedMinedBlocks }};

handle_cast({mined_block, BH, Height}, State) when State#state.miner_logging == true ->
	Message =
		io_lib:format(
			"[Stage 2/3] Produced candidate block ~s and dispatched to network.",
			[ar_util:encode(BH)]
		),
	?LOG_INFO(Message),
	ar:console("~s~n", [Message]),
	MinedBlocks = State#state.mined_blocks,
	State1 = State#state{ mined_blocks = MinedBlocks#{ Height => BH } },
	{noreply, State1};

handle_cast({mined_block, BH, Height}, State) ->
	MinedBlocks = State#state.mined_blocks,
	State1 = State#state{ mined_blocks = MinedBlocks#{ Height => BH } },
	{noreply, State1};

handle_cast({foreign_block, _BH}, #state{ no_foreign_blocks_timer = Timer } = State) ->
	{ok, cancel} = timer:cancel(Timer),
	{ok, Timer2} = timer:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), no_foreign_blocks),
	{noreply, State#state{ no_foreign_blocks_timer = Timer2 }};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(no_foreign_blocks, State) ->
	Message =
		"No foreign blocks received from the network or found by trusted peers. "
		"Please check your internet connection and the logs for errors.",
	?LOG_WARNING(Message),
	ar:console("~s~n", [Message]),
	{ok, Timer} = timer:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), no_foreign_blocks),
	{noreply, State#state{ no_foreign_blocks_timer = Timer }};

handle_info({'EXIT', _Pid, normal}, State) ->
	%% Gun sets monitors on the spawned processes, so thats the reason why we
	%% catch them here.
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
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
