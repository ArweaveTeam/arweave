-module(app_block_tx_export).
-export([export_blocks/1, export_blocks/3]).
-export([export_transactions/1, export_transactions/3]).
-include("../ar.hrl").

-record(state, {
	bhl,
	peers
}).

export_blocks(RemoteNodeAddrs) ->
	Peers = lists:map(fun ar_util:parse_peer/1, RemoteNodeAddrs),
	Filename = fun({HeightStart, HeightEnd}) ->
		"blocks-export-" ++ integer_to_list(HeightStart) ++ "-to-" ++ integer_to_list(HeightEnd) ++ ".csv"
	end,
	Ranges = [
		{0, 49999},
		{50000, 99999},
		{100000, 149999},
		{150000, 159999}
	],
	lists:map(fun(Range) ->
		export_blocks(Filename(Range), Peers, Range)
	end, Ranges).

export_blocks(Filename, Peers, {HeightStart, HeightEnd}) ->
	BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	spawn(fun() ->
		Columns = ["Height", "ID", "Timestamp", "Block Size (Bytes)", "Difficulty", "Cumulative Difficulty",
					"Reward Address", "Weave Size (Bytes)", "TXs", "TX Reward Sum (AR)", "Inflation Reward (AR)",
					"TX Mining Reward (AR)", "TX Reward Pool (AR)", "Calculated TX Reward Pool (AR)"],
		IoDevice = init_csv(Filename, Columns),
		S = #state{
			bhl = BHL,
			peers = Peers
		},
		BHs = lists:sublist(lists:reverse(BHL), HeightStart + 1, HeightEnd - HeightStart + 1),
		Fun = fun(B) ->
			io:format("Exporting block height: ~p~n", [B#block.height]),
			Values = extract_block_values(S, full_block(B)),
			ok = file:write(IoDevice, csv_encode_row(Values))
		end,
		blocks_foreach(Fun, S, BHs),
		ok = file:close(IoDevice),
		io:format("Finished!~n")
	end).

export_transactions(RemoteNodeAddrs) ->
	Peers = lists:map(fun ar_util:parse_peer/1, RemoteNodeAddrs),
	Filename = fun({HeightStart, HeightEnd}) ->
		"transactions-export-" ++ integer_to_list(HeightStart) ++ "-to-" ++ integer_to_list(HeightEnd) ++ ".csv"
	end,
	Ranges = [
		{0, 49999},
		{50000, 99999},
		{100000, 149999},
		{150000, 199999},
		{200000, 206838}
	],
	lists:map(fun(Range) ->
		export_transactions(Filename(Range), Peers, Range)
	end, Ranges).

export_transactions(Filename, Peers, {HeightStart, HeightEnd}) ->
	BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	spawn(fun() ->
		Columns = ["Block Timestamp", "TX ID", "Submitted Address",
					"Target", "Quantity (AR)", "Data Size (Bytes)", "Reward (AR)",
					"App Name", "Content Type"],
		IoDevice = init_csv(Filename, Columns),
		S = #state{
			bhl = BHL,
			peers = Peers
		},
		BHs = lists:sublist(lists:reverse(BHL), HeightStart + 1, HeightEnd - HeightStart + 1),
		WriteOneRow = fun(Values) ->
			ok = file:write(IoDevice, csv_encode_row(Values))
		end,
		Fun = fun(B) ->
			io:format("Exporting TXs for block height: ~p~n", [B#block.height]),
			lists:foreach(WriteOneRow, extract_transaction_values(full_block(B)))
		end,
		blocks_foreach(Fun, S, BHs),
		ok = file:close(IoDevice),
		io:format("Finished!~n")
	end).


%% Private

init_csv(Filename, Columns) ->
	{ok, IoDevice} = file:open(Filename, [write, exclusive]),
	ok = file:write(IoDevice, csv_encode_row(Columns)),
	IoDevice.

blocks_foreach(_, _, []) ->
	ok;
blocks_foreach(Fun, S, [BH | BHs]) ->
	{ok, B} = get_block(BH, S#state.bhl, S#state.peers),
	ok = Fun(B),
	blocks_foreach(Fun, S, BHs).

get_block(BH, BHL, Peers) ->
	case block_from_storage(BH) of
		{ok, B} ->
			{ok, B};
		_ ->
			{ok, _} = fetch_and_store_block(BH, BHL, disorder(Peers))
	end.

%% Read block from storage without hash_list and wallet_list.
block_from_storage(BH) ->
	case ar_block_index:get_block_filename(BH) of
		unavailable ->
			not_found;
		Filename ->
			{ok, Binary} = file:read_file(Filename),
			{ok, ar_serialize:json_struct_to_block(Binary)}
	end.

disorder(List) ->
	[Item || {_, Item} <- lists:sort([{rand:uniform(), Item} || Item <- List])].

fetch_and_store_block(_, _, []) ->
	{error, could_not_download};
fetch_and_store_block(BH, BHL, [Peer | Peers]) ->
	io:format("Downloading block: ~p~n", [ar_util:encode(BH)]),
	case ar_node_utils:get_full_block(Peer, BH, BHL) of
		B when ?IS_BLOCK(B) ->
			ar_storage:write_full_block(B),
			{ok, B};
		_ ->
			fetch_and_store_block(BH, BHL, Peers)
	end.

extract_block_values(S, B) ->
	{Reward, RewardPool} = reward_pool(S, B),
	[
		integer_to_binary(B#block.height),
		ar_util:encode(B#block.indep_hash),
		integer_to_binary(B#block.timestamp),
		integer_to_binary(B#block.block_size),
		integer_to_binary(B#block.diff),
		integer_to_binary(B#block.cumulative_diff),
		format_reward_addr(B#block.reward_addr),
		integer_to_binary(B#block.weave_size),
		integer_to_binary(length(B#block.txs)),
		format_float(winston_to_ar(tx_rewards(B#block.txs))),
		format_float(winston_to_ar(ar_inflation:calculate(B#block.height))),
		format_float(winston_to_ar(Reward)),
		format_float(winston_to_ar(B#block.reward_pool)),
		format_float(winston_to_ar(RewardPool))
	].

reward_pool(_, #block{ height = 0 }) ->
	{0, 0};
reward_pool(S, B) ->
	{ok, PreviousB} = get_block(B#block.previous_block, S#state.bhl, S#state.peers),
	reward_pool(S, B, PreviousB).

reward_pool(S, B, PreviousB) ->
	PreviousRecallBH = ar_node_utils:find_recall_hash(PreviousB, S#state.bhl),
	{ok, PreviousRecallB} = get_block(PreviousRecallBH, S#state.bhl, S#state.peers),
	reward_pool1(B, PreviousB, PreviousRecallB).

reward_pool1(B, PreviousB, PreviousRecallB) ->
	FullB = full_block(B),
	ar_node_utils:calculate_reward_pool(
		PreviousB#block.reward_pool,
		FullB#block.txs,
		B#block.reward_addr,
		ar_node_utils:calculate_proportion(
			PreviousRecallB#block.block_size,
			B#block.weave_size,
			B#block.height
		)
	).

full_block(B) ->
	B#block{
		txs = lists:map(fun ar_storage:read_tx/1, B#block.txs)
	}.

format_reward_addr(unclaimed) ->
	<<>>;
format_reward_addr(WalletAddr) ->
	ar_util:encode(WalletAddr).

tx_rewards(TXs) ->
	Reward = fun(TX) -> TX#tx.reward end,
	lists:sum(lists:map(Reward, TXs)).

winston_to_ar(Windston) ->
	Windston / ?WINSTON_PER_AR.

format_float(Float) ->
	float_to_binary(Float, [{decimals, 12}]).

extract_transaction_values(B) ->
	lists:map(
		fun(TX) ->
			extract_transaction_values(B, TX)
		end,
		B#block.txs
	).

extract_transaction_values(B, TX) ->
	[
		integer_to_binary(B#block.timestamp),
		ar_util:encode(TX#tx.id),
		ar_util:encode(ar_wallet:to_address(TX#tx.owner)),
		ar_util:encode(TX#tx.target),
		format_float(winston_to_ar(TX#tx.quantity)),
		integer_to_binary(byte_size(TX#tx.data)),
		format_float(winston_to_ar(TX#tx.reward)),
		extract_app_name(TX#tx.tags),
		proplists:get_value(<<"Content-Type">>, TX#tx.tags, <<>>)
	].

extract_app_name(Tags) ->
	IsAppName = fun({TagName, _}) ->
		case re:run(TagName, "^\s*app[-_]{0,1}name\s*$", [caseless]) of
			{match, _} -> true;
			nomatch -> false
		end
	end,
	FilteredTags = lists:filter(IsAppName, Tags),
	LengthSorter = fun({A, _}, {B, _}) ->
		byte_size(A) =< byte_size(B)
	end,
	SortedTags = lists:sort(LengthSorter, FilteredTags),
	case SortedTags of
		[] ->
			<<>>;
		[{_, TagValue} | _] ->
			TagValue
	end.

%% CSV

csv_encode_row(Values) ->
	iolist_to_binary(csv_encode_row(Values, [])).

csv_encode_row([], Acc) ->
	lists:reverse(Acc);
csv_encode_row([LastValue], Acc) ->
	NewAcc = [[csv_encode_value(LastValue), "\n"] | Acc],
	csv_encode_row([], NewAcc);
csv_encode_row([Value | Values], Acc) ->
	NewAcc = [[csv_encode_value(Value), ","] | Acc],
	csv_encode_row(Values, NewAcc).

csv_encode_value(String) when is_list(String) ->
	csv_encode_value(list_to_binary(String));
csv_encode_value(Value) when is_binary(Value) ->
	[
		<<"\"">>,
		binary:replace(Value, <<"\"">>, <<"\"\"">>),
		<<"\"">>
	].
