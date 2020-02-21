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
		B ->
			case [TX || TX <- ar_storage:read_tx(B#block.txs), not is_record(TX, tx)] of
				[] ->
					%% All of the TXs are found locally. Continue...
					fill_to_capacity(
						Peers,
						lists:delete(RandHash, ToWrite),
						NewBI
					);
				_ ->
					%% Download the block and its TXs...
					fill_to_capacity2(Peers, RandHash, ToWrite, NewBI)
			end
	end.

fill_to_capacity2(Peers, RandHash, ToWrite, BI) ->
	B =
		try
			ar_node_utils:get_full_block(Peers, RandHash, BI, ?WITH_TX_DATA)
		catch _:_ ->
			unavailable
		end,
	case B of
		unavailable ->
			timer:sleep(3000),
			fill_to_capacity(Peers, ToWrite, BI);
		B ->
			case ar_storage:write_full_block(B) of
				{error, _} -> disk_full;
				_ ->
					fill_to_capacity(
						Peers,
						lists:delete(RandHash, ToWrite),
						BI
					)
			end
	end.
