-module(ar_tx_validator_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({validate, TX, Peer, ReplyTo}, State) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', wallet_list},
					{'==', '$1', recent_txs_map},
					{'==', '$1', block_anchors},
					{'==', '$1', usd_to_ar_rate}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	WL = proplists:get_value(wallet_list, Props),
	RecentTXMap = proplists:get_value(recent_txs_map, Props),
	BlockAnchors = proplists:get_value(block_anchors, Props),
	USDToARRate = proplists:get_value(usd_to_ar_rate, Props),
	Wallets = ar_wallets:get(WL, ar_tx:get_addresses([TX])),
	MempoolTXs = ar_node:get_pending_txs([as_map, id_only]),
	Result = ar_tx_replay_pool:verify_tx({TX, USDToARRate, Height, BlockAnchors, RecentTXMap,
			MempoolTXs, Wallets}),
	case Result of
		valid ->
			ar_events:send(tx, {new, TX, Peer}),
			TXID = TX#tx.id,
			ar_ignore_registry:remove_temporary(TXID),
			ar_ignore_registry:add_temporary(TXID, 10 * 60 * 1000),
			case TX#tx.format of
				2 ->
					ar_data_sync:add_data_root_to_disk_pool(TX#tx.data_root, TX#tx.data_size,
							TXID);
				1 ->
					ok
			end;
		_ ->
			ok
	end,
	gen_server:reply(ReplyTo, Result),
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
