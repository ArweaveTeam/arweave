-module(app_ipfs_tests).
-include_lib("eunit/include/eunit.hrl").

add_local_and_get_test() ->
	Filename = "known_local.txt",
	DataDir = "src/apps/app_ipfs_test_data/",
	Path = DataDir ++ Filename,
	{ok, Data} = file:read_file(Path),
	TS = list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))),
	DataToHash = <<"***  *", TS/binary, "*        ", Data/binary>>,
	{ok, Hash} = ar_ipfs:add_data(DataToHash, Filename),
	ar:d({TS, Hash}),
	{ok, DataToHash} = ar_ipfs:cat_data_by_hash(Hash).

adt_simple_callback_gets_blocks_test() ->
	Node = ar_node_init(),
	{ok, Pid} = app_ipfs:start(),
	% TODO start adt callback module with gossip peers
	% TODO ... which does something testable on recv block
	Expected = mine_n_blocks_on_node(3, Node),
	ar:d({expected_hashes, Expected}),
	Actual = app_ipfs:get_block_hashes(Pid),
	?assertEqual(Expected, Actual).

%%% private

ar_node_init() ->
	ar_storage:clear(),
	B0 = ar_weave:init([]),
	Pid = ar_node:start([], B0),
	timer:sleep(1000),
	Pid.

mine_n_blocks_on_node(N, Node) ->
	lists:foreach(fun(_) ->
			ar_node:mine(Node),
			timer:sleep(500)
		end, lists:seq(1,N)),
	timer:sleep(1000),
	ar_node:get_blocks(Node).

