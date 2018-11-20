-module(ipfs_wrapper_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, 
	init_per_suite/1, end_per_suite/1,
	init_per_testcase/2, end_per_testcase/2
	]).

-export([
	add_local_and_get/1,
	adt_simple_callback_gets_block/1
	]).

all() -> [
	add_local_and_get,
	adt_simple_callback_gets_block
	].

%%%% set up

init_per_suite(Config) ->
	ar:start_for_tests(),
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(add_local_and_get, Config) ->
	Filename = "known_local.txt",
	DataDir = ?config(data_dir, Config),
	Path = DataDir ++ Filename,
	{ok, Data} = file:read_file(Path),
	[{add_local_data, {Data, Filename}} | Config];

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%% tests

adt_simple_callback_gets_block(Config) ->
	Node = ar_node_init(),
	ct:pal("Node: ~p", [Node]),
	{ok, Pid} = app_ipfs:start(),
	% TODO start adt callback module with gossip peers
	% TODO ... which does something testable on recv block
	Expected = mine_n_blocks_on_node(3, Node),
	ct:pal("Expected hashes: ~p", [Expected]),
	Actual = app_ipfs:get_block_hashes(Pid),
	?assertEqual(Expected, Actual).

add_local_and_get(Config) ->
	{Data, Filename} = ?config(add_local_data, Config),
	TS = list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))),
	DataToHash = <<"***  *", TS/binary, "*        ", Data/binary>>,
	{ok, Hash} = ar_ipfs:add_data(DataToHash, Filename),
	ct:pal("Hash at ~p: ~p", [TS, Hash]),
	{ok, DataToHash} = ar_ipfs:cat_data_by_hash(Hash).

%%% private

ar_node_init() ->
	ar_storage:clear(),
	ar_node:start().

mine_n_blocks_on_node(N, Node) ->
	lists:foreach(fun(_) ->
			ar_node:mine(Node),
			timer:sleep(500)
		end, lists:seq(1,N)),
	timer:sleep(1000),
	ar_node:get_blocks(Node).

