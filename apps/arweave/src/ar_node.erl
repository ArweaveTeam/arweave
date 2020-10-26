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
    get_trusted_peers/0, set_trusted_peers/2,
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
    add_tx/2,
    add_peers/2,
    set_reward_addr/2,
    set_loss_probability/2,
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
    block_index,
    txs = maps:new(),
    trusted_peers,
    joined = false,
    current,
    height,
    hash_list_2_0_for_1_0_blocks,
    diff,
    last_retarget,
    block_txs_pairs,
    block_cache,
    mempool_size
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
        {ok, BI} ->
            BI;
        {ok, not_joined} ->
            [];
        E ->
            ?LOG_ERROR("got unexpected value: ~w", [E])
    end.


%% @doc Get pending transactions. This includes:
%% 1. The transactions currently staying in the priority queue.
%% 2. The transactions on timeout waiting to be distributed around the network.
%% 3. The transactions ready to be and being mined.
get_pending_txs() ->
    get_pending_txs([]).

get_pending_txs(Opts) ->
    case gen_server:call(?MODULE, {get_pending_txs, Opts}) of 
        {ok, TXs} ->
            TXs;
        {ok, not_joined} ->
            [];
        E ->
            ?LOG_ERROR("got unexpected value: ~w", [E])
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

    add_peers(self(), ar_webhook:start(Config#config.webhooks)),

    %% keep it for the backward capabilities (legacy)
    ar_http_iface_server:reregister(http_entrypoint_node, self()),

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

handle_call(_Request, _From, #state{joined = false} = State) ->
    {reply, not_joined, State};

handle_call(get_blockindex, _From, State) ->
    {reply, State#state.block_index, State};

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

handle_call(get_height, _From, State) ->
    {reply, State#state.height, State};

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


%% @doc Set trusted peers.
set_trusted_peers(Proc, Peers) when is_pid(Proc) ->
    Proc ! {set_trusted_peers, Peers}.

%% @doc Set the reward address of the node.
%% This is the address mining rewards will be credited to.
set_reward_addr(Node, Addr) ->
    Node ! {set_reward_addr, Addr}.


%% @doc Set the likelihood that a message will be dropped in transmission.
%% Used primarily for testing, simulating packet loss.
set_loss_probability(Node, Prob) ->
    Node ! {set_loss_probability, Prob}.

%% @doc Add a transaction to the node server loop.
%% If accepted the tx will enter the waiting pool before being mined into the
%% the next block.
add_tx(GS, TX) when is_record(GS, gs_state) ->
    {NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
    NewGS;
add_tx(Node, TX) when is_pid(Node) ->
    Node ! {add_tx, TX},
    ok;
add_tx({Node, Name} = Peer, TX) when is_atom(Node) andalso is_atom(Name) ->
    Peer ! {add_tx, TX},
    ok;
add_tx(Host, TX) ->
    ar_http_iface_client:send_new_tx(Host, TX).

%% @doc Request to add a list of peers to the node server loop.
add_peers(Node, Peer) when not is_list(Peer) ->
    add_peers(Node, [Peer]);
add_peers(Node, Peers) ->
    Node ! {add_peers, Peers},
    ok.




%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Main server loop.
server(WPid, #{ txs := TXs, mempool_size := {MempoolHeaderSize, MempoolDataSize} } = State) ->
    receive
        stop ->
            dump_mempool(State);
        {'EXIT', _, Reason} ->
            dump_mempool(State),
            ar:info([{event, ar_node_terminated}, {reason, Reason}]);
        {sync_mempool_tx, TXID, {TX, Status} = Value} ->
            case maps:get(TXID, TXs, not_found) of
                {ExistingTX, _Status} ->
                    UpdatedTXs = maps:put(TXID, {ExistingTX, Status}, TXs),
                    server(WPid, State#{ txs => UpdatedTXs });
                not_found ->
                    UpdatedTXs = maps:put(TXID, Value, TXs),
                    {AddHeaderSize, AddDataSize} = ar_node_worker:tx_mempool_size(TX),
                    UpdatedMempoolSize =
                        {MempoolHeaderSize + AddHeaderSize, MempoolDataSize + AddDataSize},
                    server(WPid, State#{ txs => UpdatedTXs, mempool_size => UpdatedMempoolSize })
            end;
        {sync_dropped_mempool_txs, Map} ->
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
            server(WPid, State#{ txs => UpdatedTXs, mempool_size => UpdatedMempoolSize });
        {sync_reward_addr, Addr} ->
            server(WPid, State#{ reward_addr => Addr });
        {sync_trusted_peers, Peers} ->
            server(WPid, State#{ trusted_peers => Peers });
        {sync_block_cache, BlockCache} ->
            server(WPid, State#{ block_cache => BlockCache });
        {sync_state, NewState} ->
            server(WPid, NewState);
        Message ->
            server(WPid, handle(Message, WPid, State))
    end.

dump_mempool(#{ txs := TXs, mempool_size := MempoolSize }) ->
    case ar_storage:write_term(mempool, {TXs, MempoolSize}) of
        ok ->
            ok;
        {error, Reason} ->
            ar:err([{event, failed_to_persist_mempool}, {reason, Reason}])
    end.

handle(Msg, WPid, State) when is_record(Msg, gs_msg) ->
    %% We have received a gossip mesage. Gossip state manipulation is always a worker task.
    gen_server:cast(WPid, {gossip_message, Msg}),
    State;

handle({add_tx, TX}, WPid, State) ->
    gen_server:cast(WPid, {add_tx, TX}),
    State;

handle({add_peers, Peers}, WPid, State) ->
    gen_server:cast(WPid, {add_peers, Peers}),
    State;

handle({new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}, WPid, State) ->
    gen_server:cast(WPid, {process_new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}),
    State;

handle({set_loss_probability, Prob}, WPid, State) ->
    gen_server:cast(WPid, {set_loss_probability, Prob}),
    State;

handle({set_reward_addr, Addr}, WPid, State) ->
    gen_server:cast(WPid, {set_reward_addr, Addr}),
    State;

handle({work_complete, BaseBH, NewB, MinedTXs, BDS, POA, _HashesTried}, WPid, State) ->
    #{ block_index := BI } = State,
    case BI of
        not_joined ->
            do_not_cast;
        _ ->
            gen_server:cast(WPid, {
                work_complete,
                BaseBH,
                NewB,
                MinedTXs,
                BDS,
                POA
            })
    end,
    State;

handle({join, BI, Blocks}, WPid, State) ->
    #{ trusted_peers := Peers } = State,
    {ok, _} = ar_wallets:start_link([{blocks, Blocks}, {peers, Peers}]),
    ar_header_sync:join(BI, Blocks),
    ar_data_sync:join(BI),
    gen_server:cast(WPid, {join, BI, Blocks}),
    case Blocks of
        [B] ->
            ar_header_sync:add_block(B);
        _ ->
            ok
    end,
    State;


handle({set_trusted_peers, Peers}, WPid, State) ->
    gen_server:cast(WPid, {set_trusted_peers, Peers}),
    State;

