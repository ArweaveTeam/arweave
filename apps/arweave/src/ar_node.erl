%% This Source Code Form is subject to the terms of the GNU General 
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed 
%% with this file, You can obtain one at 
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
%% @author Sam Williams <sam@arweave.org>
%% @author Lev Berman <lev@arweave.org>
%% @author Taras Halturin <taras@arweave.org>
%%
%% @doc

-module(ar_node).

-behavior(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([
    get_blocks/0,
    get_block_index/0, is_in_block_index/1, get_height/0,
    get_trusted_peers/0, set_trusted_peers/1,
    get_balance/1,
    get_last_tx/1,
    get_wallets/1,
    get_wallet_list_chunk/2,
    get_current_diff/0, get_diff/0,
    get_pending_txs/0, get_pending_txs/1, get_mined_txs/0, is_a_pending_tx/1,
    get_current_block_hash/0,
    get_block_index_entry/1,
    get_2_0_hash_of_1_0_block/1,
    is_joined/0,
    get_block_txs_pairs/0,
    mine/0, 
    add_tx/1,
    add_peers/1,
    set_reward_addr/1,
    set_loss_probability/1,
    get_mempool_size/0,
    get_block_shadow_from_cache/1,
    get_search_space_upper_bound/1
]).

-include("ar.hrl").
-include("ar_mine.hrl").
-include("ar_config.hrl").
-include("common.hrl").

%% Internal state definition.
-record(state, {
    block_index = not_joined,
    txs = maps:new(),
    trusted_peers,
    current = not_joined,
    height = -1,
    hash_list_2_0_for_1_0_blocks,
    diff,
    last_retarget,
    block_txs_pairs,
    block_cache,
    mempool_size,
    reward_addr,
    reward_pool
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_blocks() ->
    get_block_index().

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_block_index() ->
    case gen_server:call(?MODULE, get_blockindex) of 
        not_joined ->
            [];
        BI ->
            BI
    end.


%% @doc Get pending transactions. This includes:
%% 1. The transactions currently staying in the priority queue.
%% 2. The transactions on timeout waiting to be distributed around the network.
%% 3. The transactions ready to be and being mined.
get_pending_txs() ->
    get_pending_txs([]).

get_pending_txs(Opts) ->
    case gen_server:call(?MODULE, {get_pending_txs, Opts}) of 
        not_joined ->
            [];
        TXs ->
            TXs
    end.

%% @doc Return true if a tx with the given identifier is pending.
is_a_pending_tx(TXID) ->
    gen_server:call(?MODULE, {is_a_pending_tx, TXID}).

%% @doc Get the list of mined or ready to be mined transactions.
%% The list does _not_ include transactions in the priority queue or
%% those on timeout waiting for network propagation.
get_mined_txs() ->
    gen_server:call(?MODULE, get_mined_txs).

%% @doc Get trusted peers.
get_trusted_peers() ->
    gen_server:call(?MODULE, get_trusted_peers).

%% @doc Return true if the given block hash is found in the block index.
is_in_block_index(H) ->
    gen_server:call(?MODULE, {is_in_block_index, H}).

%% @doc Get the current block hash.
get_current_block_hash() ->
    gen_server:call(?MODULE, get_current_block_hash).

%% @doc Get the block index entry by height.
get_block_index_entry(Height) ->
    gen_server:call(?MODULE, {get_block_index_entry, Height}).

%% @doc Get the 2.0 hash for a 1.0 block.
%% Before 2.0, to compute a block hash, the complete wallet list
%% and all the preceding hashes were required. Getting a wallet list
%% and a hash list for every historical block to verify it belongs to
%% the weave is very costly. Therefore, a list of 2.0 hashes for 1.0
%% blocks was computed and stored along with the network client.
get_2_0_hash_of_1_0_block(Height) ->
    gen_server:call(?MODULE, {get_2_0_hash_of_1_0_block, Height}).

%% @doc Return the current height of the blockweave.
get_height() ->
    gen_server:call(?MODULE, get_height).

%% @doc Check whether the node has joined the network.
is_joined() ->
    case gen_server:call(?MODULE, get_current_block_hash) of 
        not_joined -> false;
        _ -> true
    end.

%% @doc Returns the estimated future difficulty of the currently mined block.
%% The function name is confusing and needs to be changed.
get_current_diff() ->
    gen_server:call(?MODULE, get_current_diff).

%% @doc Returns the difficulty of the current block (the last applied one).
get_diff() ->
    gen_server:call(?MODULE, get_diff).

%% @doc Returns transaction identifiers from the last ?MAX_TX_ANCHOR_DEPTH
%% blocks grouped by block hash.
get_block_txs_pairs() ->
    gen_server:call(?MODULE, get_block_txs_pairs).

%% @doc Return memory pool size
get_mempool_size() ->
    gen_server:call(?MODULE, get_mempool_size).

%% @doc Get the block shadow from the block cache.
get_block_shadow_from_cache(H) ->
    gen_server:call(?MODULE, {get_block_shadow_from_cache, H}).

%% @doc Get the upper bound of the SPoRA search space of the block of the given height.
get_search_space_upper_bound(Height) ->
    gen_server:call(?MODULE, {get_search_space_upper_bound, Height}).

%% @doc Get the current balance of a given wallet address.
%% The balance returned is in relation to the nodes current wallet list.
get_balance(Addr) when ?IS_ADDR(Addr) ->
    ar_wallets:get_balance(Addr);
get_balance(WalletID) ->
    get_balance(ar_wallet:to_address(WalletID)).

%% @doc Get the last tx id associated with a given wallet address.
%% Should the wallet not have made a tx the empty binary will be returned.
get_last_tx(Addr) when ?IS_ADDR(Addr) ->
    {ok, ar_wallets:get_last_tx(Addr)};
get_last_tx(WalletID) ->
    get_last_tx(ar_wallet:to_address(WalletID)).

%% @doc Return a map address => {balance, last tx} for the given addresses.
get_wallets(Addresses) ->
    ar_wallets:get(Addresses).

%% @doc Return a chunk of wallets from the tree with the given root hash starting
%% from the Cursor address.
get_wallet_list_chunk(RootHash, Cursor) ->
    ar_wallets:get_chunk(RootHash, Cursor).


%% @doc Trigger a node to start mining a block.
mine() ->
    gen_server:cast(ar_node_worker, mine).

%% @doc Add a transaction to the node server loop.
%% If accepted the tx will enter the waiting pool before being mined into the
%% the next block.
add_tx(TX)->
    gen_server:cast(ar_node_worker, {add_tx, TX}).


%% @doc Request to add a list of peers to the node server loop.
add_peers(Peer) when not is_list(Peer) ->
    add_peers([Peer]);
add_peers(Peers) ->
    gen_server:cast(ar_node_worker, {add_peers, Peers}).

%% @doc Set the likelihood that a message will be dropped in transmission.
%% Used primarily for testing, simulating packet loss.
set_loss_probability(Prob) ->
    gen_server:cast(ar_node_worker, {set_loss_probability, Prob}).

%% @doc Set the reward address of the node.
%% This is the address mining rewards will be credited to.
set_reward_addr(Addr) ->
    gen_server:cast(ar_node_worker, {set_reward_addr, Addr}).

%% @doc Set trusted peers.
set_trusted_peers(Peers) ->
    gen_server:cast(ar_node_worker, {set_trusted_peers, Peers}).

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

    {ok, Config} = application:get_env(arweave, config),
    Peers = ar_join:filter_peer_list(Config#config.peers),

    add_peers(ar_webhook:start(Config#config.webhooks)),

    % FIXME remove it once it become gen_server
    %% RandomX initializing
    ar_randomx_state:start(),
    ar_randomx_state:start_block_polling(),

    {ok, #state{trusted_peers = Peers}}.

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

handle_call({get_pending_txs, Opts}, _From, State) ->
    Reply =
        case {lists:member(as_map, Opts), lists:member(id_only, Opts)} of
            {true, false} ->
                State#state.txs;
            {true, true} ->
                maps:map(fun(_TXID, _Value) -> no_tx end, State#state.txs);
            {false, true} ->
                maps:keys(State#state.txs);
            {false, false} ->
                maps:fold(
                    fun(_, {TX, _}, Acc) ->
                        [TX | Acc]
                    end,
                    [],
                   State#state.txs 
                )
        end,
    {reply, Reply, State};

handle_call(get_height, _From, State) ->
    {reply, State#state.height, State};

handle_call(_Request, _From, #state{current = not_joined} = State) ->
    {reply, not_joined, State};

handle_call(get_blockindex, _From, State) ->
    {reply, State#state.block_index, State};


handle_call(get_mined_txs, _From, State) ->
    MinedTXs = maps:fold(
        fun
            (_, {TX, ready_for_mining}, Acc) ->
                [TX | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        State#state.txs 
    ),
    {reply, MinedTXs, State};

handle_call({is_a_pending_tx, TXID}, _From, State) ->
    {reply, maps:is_key(TXID, State#state.txs), State};

handle_call(get_trusted_peers, _From, State) ->
    {reply, State#state.trusted_peers, State};

handle_call({is_in_block_index, H}, _From, State) ->
    BI = State#state.block_index,
    Reply =
        case lists:search(fun({BH, _, _}) -> BH == H end, BI) of
            {value, _} ->
                true;
            false ->
                false
        end,
    {reply, Reply, State};

handle_call(get_current_block_hash, _From, State) ->
    {reply, State#state.current, State};

handle_call({get_block_index_entry, Height}, _From, State) ->
    CurrentHeight = State#state.height,
    BI = State#state.block_index,
    Reply =
        case Height > CurrentHeight of
            true ->
                not_found;
            false ->
                lists:nth(CurrentHeight - Height + 1, BI)
        end,
    {reply, Reply, State}; 

handle_call({get_2_0_hash_of_1_0_block, Height}, _From, State) ->
    HL = State#state.hash_list_2_0_for_1_0_blocks,
    Fork_2_0 = ar_fork:height_2_0(),
    Reply =
        case Height > Fork_2_0 of
            true ->
                invalid_height;
            false ->
                lists:nth(Fork_2_0 - Height, HL)
        end,
    {reply, Reply, State};


handle_call(get_current_diff, _From, State) ->
    Height = State#state.height,
    Diff = State#state.diff,
    LastRetarget = State#state.last_retarget,

    Reply = ar_retarget:maybe_retarget(
                Height + 1,
                Diff,
                os:system_time(seconds),
                LastRetarget
            ),

    {reply, Reply, State}; 

handle_call(get_diff, _From, State) ->
    {reply, State#state.diff, State};

handle_call(get_block_txs_pairs, _From, State) ->
    {reply, State#state.block_txs_pairs, State};

handle_call(get_mempool_size, _From, State) ->
    {reply, State#state.mempool_size, State};

handle_call({get_block_shadow_from_cache, H}, _From, State) ->
    Reply = ar_block_cache:get(State#state.block_cache, H),
    {reply, Reply, State};

handle_call({get_search_space_upper_bound, Height}, _From, State) ->
    BI = State#state.block_index,
    CurrentHeight = State#state.height,

    TargetHeight = Height - ?SEARCH_SPACE_UPPER_BOUND_DEPTH(Height),

    WeaveSize =
        case TargetHeight > CurrentHeight of
            true ->
                {error, height_out_of_range};
            false ->
                Index = CurrentHeight - TargetHeight + 1,
                case Index > length(BI) of
                    true ->
                        element(2, lists:last(BI));
                    false ->
                        element(2, lists:nth(CurrentHeight - TargetHeight + 1, BI))
                end
        end,

    {reply, WeaveSize, State};

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
handle_info({sync_mempool_tx, TXID, {TX, Status} = Value}, State) ->
    {MempoolHeaderSize, MempoolDataSize} = State#state.mempool_size,
    TXs = State#state.txs,

    case maps:get(TXID, TXs, not_found) of

        {ExistingTX, _Status} ->
            UpdatedTXs = maps:put(TXID, {ExistingTX, Status}, TXs),
            {noreply, State#state{ txs = UpdatedTXs }};

        not_found ->
            UpdatedTXs = maps:put(TXID, Value, TXs),
            {AddHeaderSize, AddDataSize} = ar_node_worker:tx_mempool_size(TX),

            UpdatedMempoolSize = {
                MempoolHeaderSize + AddHeaderSize, 
                MempoolDataSize + AddDataSize
            },

            State1 = State#state{
                       txs = UpdatedTXs,
                       mempool_size = UpdatedMempoolSize
                     },

            {noreply, State1}
    end;

handle_info({sync_dropped_mempool_txs, Map}, State) ->
    {MempoolHeaderSize, MempoolDataSize} = State#state.mempool_size,
    TXs = State#state.txs,

    {UpdatedTXs, UpdatedMempoolSize} =
        maps:fold(
            fun(TXID, {TX, _Status}, {MapAcc, MempoolSizeAcc} = Acc) ->
                case maps:is_key(TXID, MapAcc) of
                    true ->
                        {DroppedHeaderSize, DroppedDataSize} =
                            ar_node_worker:tx_mempool_size(TX),
                        {HeaderSize, DataSize} = MempoolSizeAcc,
                        UpdatedMempoolSizeAcc =
                            {HeaderSize - DroppedHeaderSize, DataSize - DroppedDataSize},
                        {maps:remove(TXID, MapAcc), UpdatedMempoolSizeAcc};
                    false ->
                        Acc
                end
            end,
            {TXs, {MempoolHeaderSize, MempoolDataSize}},
            Map
        ),

    State1 = State#state{
        txs = UpdatedTXs,
        mempool_size = UpdatedMempoolSize 
    },

    {noreply, State1};

handle_info({sync_reward_addr, Addr}, State) ->
    {noreply, State#state{ reward_addr = Addr}};

handle_info({sync_trusted_peers, Peers}, State) ->
    {noreply, State#state{ trusted_peers = Peers }};

handle_info({sync_block_cache, BlockCache}, State) ->
    {noreply, State#state{ block_cache = BlockCache }};

handle_info({sync_state, NewStateValues}, State) ->
    #{
        block_index     := BI,
        current         := Current,
        txs             := TXs,
        mempool_size    := MemPoolSize,
        height          := Height,
        reward_pool     := RewardPool,
        diff            := Diff,
        last_retarget   := LastRetarget,
        block_txs_pairs := BlockTXsPairs,
        block_cache     := BlockCache
     } = NewStateValues,

    State1 = State#state{
                block_index = BI,
                txs = TXs,
                current = Current,
                height = Height,
                diff = Diff,
                last_retarget = LastRetarget,
                block_txs_pairs = BlockTXsPairs,
                block_cache = BlockCache,
                mempool_size = MemPoolSize,
                reward_pool = RewardPool
              },

    {noreply, State1};

handle_info(Info, State) when is_record(Info, gs_msg) ->
    %% We have received a gossip mesage. Gossip state manipulation is always a worker task.
    gen_server:cast(ar_node_worker, {gossip_message, Info}),
    {noreply, State};

handle_info({new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}, State) ->
    gen_server:cast(ar_node_worker, {process_new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}),
    {noreply, State};

handle_info({work_complete, BaseBH, NewB, MinedTXs, BDS, POA, _HashesTried}, State) ->
    case State#state.current of
        not_joined ->
            {noreply, State};
        _ ->
            gen_server:cast(ar_node_worker, {
                work_complete,
                BaseBH,
                NewB,
                MinedTXs,
                BDS,
                POA
            }),

            {noreply, State}
    end;

handle_info({join, BI, Blocks}, State) ->
    Peers = State#state.trusted_peers,

    %FIXME we should move this gen_server under the supervisor
    {ok, _} = ar_wallets:start_link([{blocks, Blocks}, {peers, Peers}]),

    ar_header_sync:join(BI, Blocks),
    ar_data_sync:join(BI),

    case Blocks of
        [B] ->
            ar_header_sync:add_block(B);
        _ ->
            ok
    end,

    gen_server:cast(ar_node_worker, {join, BI, Blocks}),
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
terminate(normal, State) ->
    dump_mempool(State#state.txs, State#state.mempool_size);

terminate(Reason, State) ->
    ?LOG_WARNING("~p has been terminated with reason: ~p", [?MODULE, Reason]),
    terminate(normal, State).

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
%%% Private functions.
%%%===================================================================

dump_mempool(TXs, MempoolSize) ->
    case ar_storage:write_term(mempool, {TXs, MempoolSize}) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_ERROR("failed to dump mempool: ~p", [Reason])
    end.





