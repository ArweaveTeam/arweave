-module(ar_downloader).

-export([start/2, add_block/2, add_block/3]).

-include("ar.hrl").

%%% This module contains the core transaction and block downloader.
%%% After the node has joined the network, this process should be started,
%%% which continually downloads data until either the drive is full
%%% or the entire weave has been mirrored.

start(Peers, BI) ->
    PID = spawn(fun() -> fill_to_capacity(Peers, BI) end),
    register(?MODULE, PID).

add_block(B, BI) -> add_block(B#block.indep_hash, BI, whereis(?MODULE)).
add_block(BH, BI, PID) ->
	case PID of
		undefined ->
			ar:info(
				[
					not_adding_txs_to_downloader,
					{block, BH}
				]
			);
		_ ->
			PID ! {block, BH, BI}
	end.

%% @doc Fills node to capacity based on weave storage limit.
fill_to_capacity(Peers, BI) ->
	fill_to_capacity(Peers, ?BI_TO_BHL(BI), BI).
fill_to_capacity(Peers, [], _BI) ->
	io:format("Out of data to download. Waiting for more hashes...~n"),
	receive
		{block, H, NewBI} ->
			fill_to_capacity(Peers, [H], NewBI)
	end;
fill_to_capacity(Peers, ToWrite, BI) ->
	{NewBI, NewToWrite} =
		receive
			{block, H, XBI} -> {XBI, [H | ToWrite]}
		after 0 -> {BI, ToWrite}
		end,
	timer:sleep(250),
	RandHash = lists:nth(rand:uniform(length(NewToWrite)), NewToWrite),
	case ar_storage:read_block(RandHash, NewBI) of
		unavailable ->
			%% Block is not found locally, download it and its transactions.
			fill_to_capacity2(Peers, RandHash, ToWrite, NewBI);
		B = #block{txs = TXS0} ->
			{TXS, TXIDs} =
				lists:partition(fun(TX) -> is_record(TX, tx) end,
					ar_storage:read_tx(TXS0)),

			case {TXS, TXIDs} of
				{[], []} ->
					%% Download the block and its TXs...
					fill_to_capacity2(Peers, RandHash, ToWrite, NewBI);
				{[_|_], []} ->
					%% All of the TXs are found locally. Process format 2 TXs
					handle_format_2_txs_and_continue(
						Peers,
						TXS,
						[],
						B,
						RandHash,
						ToWrite,
						NewBI,
						get(write_format_2_block));
				{_, [_|_]} ->
					%% Download missing TXs and fill in block
					fill_to_capacity3(Peers, B, TXIDs, RandHash, ToWrite, BI)
			end
	end.

fill_to_capacity2(Peers, RandHash, ToWrite, BI) ->
	B =
		try
			ar_node_utils:get_block(Peers, RandHash, BI, ?WITH_TX_DATA)
		catch _:_ ->
			unavailable
		end,
	write_full_block_and_continue(Peers, B, RandHash, ToWrite, BI).

fill_to_capacity3(Peers, B = #block{txs = TXS}, TXIDs, RandHash, ToWrite, BI) ->
	FilteredTXS = TXS -- TXIDs,
	try
		FullTXS =
			lists:foldl(
				fun(TXID, Acc) ->
					[get_tx(Peers, TXID) | Acc]
				end, FilteredTXS, TXIDs),
		write_full_block_and_continue(Peers, B#block{txs = FullTXS}, RandHash, ToWrite, BI)
	catch
		_:{not_found, _} ->
			fill_to_capacity2(Peers, RandHash, ToWrite, BI)
	end.

write_full_block_and_continue(Peers, unavailable, _RandHash, ToWrite, BI) ->
	timer:sleep(3000),
	fill_to_capacity(Peers, ToWrite, BI);
write_full_block_and_continue(Peers, B, RandHash, ToWrite, BI) ->
	case ar_storage:write_full_block(B) of
		{error, _} -> disk_full;
		_ ->
			ar_bridge:drop_downloaded_txs(whereis(http_bridge_node),
				[TX#tx.id || TX = #tx{} <- B#block.txs]),
			fill_to_capacity(
				Peers,
				lists:delete(RandHash, ToWrite),
				BI
			)
	end.

handle_format_2_txs_and_continue(Peers, [], Acc, B, RandHash, ToWrite, BI, true) ->
	write_full_block_and_continue(
		Peers, B#block{txs = Acc}, RandHash, ToWrite, BI);
handle_format_2_txs_and_continue(Peers, [], _Acc, _B, RandHash, ToWrite, BI, _Flag) ->
	fill_to_capacity(
		Peers,
		lists:delete(RandHash, ToWrite),
		BI);
handle_format_2_txs_and_continue(
	Peers, [TX | Rem], Acc, B, RandHash, ToWrite, BI, Flag) ->
	#tx{id = TXID, format = TXFormat, data = TXData, data_size = TXSize} = TX,
	{TX1, NFlag} =
		case TXData of
			<<>> when (TXSize > 0) and (TXFormat =:= 2) ->
				try
					TX0 = get_tx(Peers, TXID),
					ok = maybe_update_write_block_flag(Flag, OFlag = true),
					{TX0, OFlag}
				catch
					_:{not_found} -> {TX, Flag}
				end;
			_Any ->
				{TX, Flag}
		end,
	handle_format_2_txs_and_continue(
		Peers, Rem, [TX1 | Acc], B, RandHash, ToWrite, BI, NFlag).

get_tx(Peers, TXID) ->
	case ar_http_iface_client:get_tx_from_remote_peer(Peers, TXID, ?WITH_TX_DATA) of
		not_found = Error ->
			%% Transaction not_found in both local storage & remote
			%% peer, exit iteration and download full block.
			throw({Error, TXID});
		TX = #tx{} ->
			TX
	end.

maybe_update_write_block_flag(Flag, Flag) -> ok;
maybe_update_write_block_flag(_, Flag) ->
	put(write_format_2_block, Flag),
	ok.
