%% This Source Code Form is subject to the terms of the GNU General 
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed 
%% with this file, You can obtain one at 
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
%% @author Taras Halturin
%%
%% @doc Watchdog process. Monitors some states and triggers according actions
%% i.e. logging, casting another processes etc.

-module(ar_watchdog).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([started_hashing/0,
         block_received_n_confirmations/2,
         mined_block/2,
         foreign_block/1]).

%% includes
-include_lib("ar.hrl").
-include_lib("common.hrl").


%% records
-record(state, {
    mined_blocks,
    last_foreign_block = 0,
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
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    MinerLogging = case ar_meta_db:get(miner_logging) of 
        true -> true;
        _ -> false
    end,

    State = #state{ 
        mined_blocks = maps:new(),
        miner_logging = MinerLogging
    },

    erlang:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), process),
    {ok, State}.

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
handle_call(Request, _From, State) ->
    ?LOG_ERROR("unhandled call: ~p", [Request]),
    {reply, ok, State}.


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
handle_cast(started_hashing, State) when State#state.miner_logging == true ->
    ?LOG_INFO("[Stage 1/3] Successfully proved access to recall block. Starting to hash."),
    {noreply, State};

handle_cast(started_hashing, State) -> 
    {noreply, State};

handle_cast({block_received_n_confirmations, BH, Height}, State) ->
    MinedBlocks = State#state.mined_blocks,
    UpdatedMinedBlocks = case maps:take(Height, MinedBlocks) of
        {BH, Map} when State#state.miner_logging == true ->
            %Log the message for block mined by the local node 
            %got confirmed by the network
            ?LOG_INFO("[Stage 3/3] Your block ~s was accepted by the network!", 
                      [ar_util:encode(BH)]),
            Map;
        {_, Map} ->
            Map;
        error ->
            MinedBlocks
    end,
    {noreply, State#state{mined_blocks = UpdatedMinedBlocks}};

handle_cast({mined_block, BH, Height}, State) when State#state.miner_logging == true ->
    ?LOG_INFO(
        "[Stage 2/3] Produced candidate block ~s and dispatched to network.",
        [ar_util:encode(BH)]
    ),
    MinedBlocks = State#state.mined_blocks,
    State1 = State#state{mined_blocks = MinedBlocks#{ Height => BH }},
    {noreply, State1};

handle_cast({mined_block, BH, Height}, State) ->
    MinedBlocks = State#state.mined_blocks,
    State1 = State#state{mined_blocks = MinedBlocks#{ Height => BH }},
    {noreply, State1};

handle_cast({foreign_block, BH}, State) ->
    {noreply, State#state{last_foreign_block = BH}};

handle_cast(Msg, State) ->
    ?LOG_ERROR("unhandled cast: ~p", [Msg]),
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
handle_info(process, State) when State#state.last_foreign_block > 0 ->
    erlang:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), process),
    {noreply, State#state{last_foreign_block = 0}};

handle_info(process, State) ->
    ?LOG_WARNING(
        "No foreign blocks received from the network or found by trusted peers. "
        "Please check your internet connection and the logs for errors."
    ),
    erlang:send_after(?FOREIGN_BLOCK_ALERT_TIME, self(), process),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG_ERROR("unhandled info: ~p", [Info]),
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



