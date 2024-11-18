-module(ar_block_time_history).

-export([history_length/0, has_history/1, get_history/1, get_history_from_blocks/2, 
	set_history/2, get_hashes/1, sum_history/1, compute_block_interval/1,
	validate_hashes/2, hash/1, update_history/2]).

-include_lib("arweave/include/ar.hrl").

-ifdef(DEBUG).
	-define(BLOCK_TIME_HISTORY_BLOCKS, 3).
-else.
	-ifndef(BLOCK_TIME_HISTORY_BLOCKS).
		-define(BLOCK_TIME_HISTORY_BLOCKS, (30 * 24 * 30)).
	-endif.
-endif.

history_length() ->
	?BLOCK_TIME_HISTORY_BLOCKS.

has_history(Height) ->
	Height - history_length() > ar_fork:height_2_7().

get_history(B) ->
	lists:sublist(B#block.block_time_history, history_length()).

get_history_from_blocks([], _PrevB) ->
	[];
get_history_from_blocks([B | Blocks], PrevB) ->
	case B#block.height >= ar_fork:height_2_7() of
		false ->
			get_history_from_blocks(Blocks, B);
		true ->
			[{B#block.indep_hash, get_history_element(B, PrevB)}
					| get_history_from_blocks(Blocks, B)]
	end.

set_history([], _History) ->
	[];
set_history(Blocks, []) ->
	Blocks;
set_history([B | Blocks], History) ->
	[B#block{ block_time_history = History } | set_history(Blocks, tl(History))].

get_hashes(Blocks) ->
	TipB = hd(Blocks),
	Len = min(TipB#block.height - ar_fork:height_2_7() + 1, ?STORE_BLOCKS_BEHIND_CURRENT),
	[B#block.block_time_history_hash || B <- lists:sublist(Blocks, Len)].

sum_history(B) ->
	{IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount} =
		lists:foldl(
			fun({BlockInterval, VDFInterval, ChunkCount}, {Acc1, Acc2, Acc3, Acc4}) ->
				{
					Acc1 + BlockInterval,
					Acc2 + VDFInterval,
					case ChunkCount of
						1 -> Acc3 + 1;
						_ -> Acc3
					end,
					case ChunkCount of
						1 -> Acc4;
						_ -> Acc4 + 1
					end
				}
			end,
			{0, 0, 0, 0},
			get_history(B)
		),
	{IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount}.

compute_block_interval(B) ->
	Height = B#block.height + 1,
	case has_history(Height) of
		true ->
			IntervalTotal =
				lists:foldl(
					fun({BlockInterval, _VDFInterval, _ChunkCount}, Acc) ->
						Acc + BlockInterval
					end,
					0,
					get_history(B)
				),
			IntervalTotal div history_length();
		false -> 120
	end.

validate_hashes(_History, []) ->
	true;
validate_hashes(History, [H | Hashes]) ->
	case validate_hash(H, History) of
		true ->
			validate_hashes(tl(History), Hashes);
		false ->
			false
	end.

validate_hash(H, History) ->
	H == hash(History).

hash(History) ->
	History2 = lists:sublist(History, history_length()),
	hash(History2, [ar_serialize:encode_int(length(History2), 8)]).

hash([], IOList) ->
	crypto:hash(sha256, iolist_to_binary(IOList));
hash([{BlockInterval, VDFInterval, ChunkCount} | History], IOList) ->
	BlockIntervalBin = ar_serialize:encode_int(BlockInterval, 8),
	VDFIntervalBin = ar_serialize:encode_int(VDFInterval, 8),
	ChunkCountBin = ar_serialize:encode_int(ChunkCount, 8),
	hash(History, [BlockIntervalBin, VDFIntervalBin, ChunkCountBin | IOList]).

update_history(B, PrevB) ->
	case B#block.height >= ar_fork:height_2_7() of
		false ->
			PrevB#block.block_time_history;
		true ->
			[get_history_element(B, PrevB) | PrevB#block.block_time_history]
	end.

get_history_element(B, PrevB) ->
	BlockInterval = max(1, B#block.timestamp - PrevB#block.timestamp),
	VDFInterval = ar_block:vdf_step_number(B) - ar_block:vdf_step_number(PrevB),
	ChunkCount =
		case B#block.recall_byte2 of
			undefined ->
				1;
			_ ->
				2
		end,
	{BlockInterval, VDFInterval, ChunkCount}.
