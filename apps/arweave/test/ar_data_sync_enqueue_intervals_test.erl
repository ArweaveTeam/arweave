-module(ar_data_sync_enqueue_intervals_test).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").

enqueue_intervals_test() ->
	test_enqueue_intervals([], 2, [], [], [], "Empty Intervals"),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {101, 102, 103, 104, 1984},
	Peer3 = {201, 202, 203, 204, 1984},

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
				]), none}
		],
		5,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
			{none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer1},
			{none, 8*?DATA_CHUNK_SIZE, 9*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, all chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
				]), none}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, 2 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			]), none},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			]), none}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2},
			{none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			]), none},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			]), none}
		],
		3,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
			{none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
		],
		"Multiple peers, overlapping, full intervals, 3 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none}
		],
		5,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, all chunks. Overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			]), none},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			]), none}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			]), none}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1}
		],
		"Single peer, partial intervals, 2 chunks. Overlapping partial QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			]), none},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			]), none}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1},
			{none, trunc(3.25*?DATA_CHUNK_SIZE), 4*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, trunc(6.5*?DATA_CHUNK_SIZE), Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals.").

test_enqueue_intervals(Intervals, ChunksPerPeer, QIntervalsRanges, ExpectedQIntervalRanges, ExpectedChunks, Label) ->
	QIntervals = ar_intervals:from_list(QIntervalsRanges),
	Q = gb_sets:new(),
	{QResult, QIntervalsResult} = ar_data_sync:enqueue_intervals(Intervals, ChunksPerPeer, {Q, QIntervals}),
	ExpectedQIntervals = lists:foldl(fun({End, Start}, Acc) ->
			ar_intervals:add(Acc, End, Start)
		end, QIntervals, ExpectedQIntervalRanges),
	?assertEqual(ar_intervals:to_list(ExpectedQIntervals), ar_intervals:to_list(QIntervalsResult), Label),
	?assertEqual(ExpectedChunks, gb_sets:to_list(QResult), Label). 