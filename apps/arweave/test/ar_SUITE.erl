%%%
%%% @doc Arweave black-box tests.
%%%

-module(ar_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Tests.
%%%===================================================================

-export([
	mod_exist/1,
	ar_data_sync_fork_recovery/1, % ar_data_sync.erl
	events_subscribe_send_cancel/1, % ar_events.erl
	events_process_terminated/1
]).

%%%===================================================================
%%% Common Test callback functions.
%%%===================================================================

-export([
	suite/0,
	init_per_suite/1,
	end_per_suite/1,
	init_per_group/2,
	end_per_group/2,
	init_per_testcase/2,
	end_per_testcase/2,
	groups/0,
	all/0
]).

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
	[{timetrap, {seconds, 30}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip, Reason} | {skip_and_save, Reason, Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
	%% Clean the data dir before we start this suite if it hasn't been yet for some reason.
	os:cmd("rm -r " ++ ?config(data_dir, Config) ++ "/*"),
	Slave = start_slave_node(),
	[{slave, Slave} | Config].

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config, Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(Config) ->
	stop_slave_node(Config),
	ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip, Reason} | {skip_and_save, Reason, Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
	Config1 = start_master_application(Config),
	start_slave_application(Config1).

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config, Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_GroupName, Config) ->
	stop_slave_application(Config),
	stop_master_application(Config).

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip, Reason} | {skip_and_save, Reason, Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
	Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config, Config1} | {fail, Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
	Config.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() ->
    [
    {nodeBasic,[sequence], [
			ar_data_sync_fork_recovery,
			events_subscribe_send_cancel,
			events_process_terminated
        ]}
    ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip, Reason}
%%
%% GroupsAndTestCases = [{group, GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%% Reason = term()
%%   The reason for skipping all groups and test cases.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%--------------------------------------------------------------------
all() ->
	[
		mod_exist,
		{group, nodeBasic}
	].

%% Tests.
%%
%% Note! Please, dont use this module for the tests. Wrap it up by the
%% short function and put the real test implementation into the external module.
%% Lets keep this module clean.
mod_exist(Config) ->
	{module, ar_node} = code:load_file(ar_node),
	{module, ar_events} = code:load_file(ar_events),
	Config.

ar_data_sync_fork_recovery(Config) -> ar_ct_data_sync:fork_recovery(Config).
events_subscribe_send_cancel(Config) -> ar_test_events:subscribe_send_cancel(Config).
events_process_terminated(Config) -> ar_test_events:process_terminated(Config).

%%%===================================================================
%%% Private functions.
%%%===================================================================

start_slave_node() ->
	{ok, Slave} = ct_slave:start('slave', [{monitor_master, true}]),
	ok = ct_rpc:call(Slave, code, add_pathsz, [code:get_path()]),
	Slave.

stop_slave_node(Config) ->
	ct_slave:stop(?config(slave, Config)).

start_master_application(Config) ->
	ApplicationConfig = #config{
		start_from_block_index = true,
		data_dir = ?config(data_dir, Config) ++ "master",
		metrics_dir = "metrics_master"
	},
	ok = application:set_env(arweave, config, ApplicationConfig),
	ar_storage:ensure_directories(ApplicationConfig#config.data_dir),
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0 | _] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	{ok, WalletList} = ar_storage:read_wallet_list(B0#block.wallet_list),
	write_genesis_files(ApplicationConfig#config.data_dir, B0, WalletList),
	ar_test_lib:start_test_application(),
	lists:append([{wallet, Wallet}, {'B0', B0}], Config).

stop_master_application(Config) ->
	ar_test_lib:stop_test_application(),
	Config1 = proplists:delete(wallet, Config),
	proplists:delete('B0', Config1).

start_slave_application(Config) ->
	ApplicationConfig = #config{
		start_from_block_index = false,
		port = 1983,
		peers = [{127, 0, 0, 1, 1984}],
		data_dir = ?config(data_dir, Config) ++ "slave",
		metrics_dir = "metrics_slave"
	},
	ar_storage:ensure_directories(ApplicationConfig#config.data_dir),
	start_slave_application(Config, ApplicationConfig).

start_slave_application(Config, ApplicationConfig) ->
	Slave = ?config(slave, Config),
	ok = ct_rpc:call(Slave, application, set_env, [arweave, config, ApplicationConfig]),
	ok = ct_rpc:call(Slave, ar_test_lib, start_test_application, []),
	Config.

stop_slave_application(Config) ->
	Slave = ?config(slave, Config),
	ct_rpc:call(Slave, ar_test_lib, stop_test_application, []).

write_genesis_files(DataDir, B0, WalletList) ->
	%% Make sure all required directories exist.
	ar_storage:ensure_directories(DataDir),
	%% Write genesis block.
	BH = B0#block.indep_hash,
	BlockDir = filename:join(DataDir, ?BLOCK_DIR),
	BlockFilepath = filename:join(BlockDir, binary_to_list(ar_util:encode(BH)) ++ ".json"),
	BlockJSON = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B0)),
	ok = file:write_file(BlockFilepath, BlockJSON),
	%% Write genesis transactions.
	TXDir = filename:join(DataDir, ?TX_DIR),
	lists:foreach(
		fun(TX) ->
			TXID = TX#tx.id,
			TXFilepath = filename:join(TXDir, binary_to_list(ar_util:encode(TXID)) ++ ".json"),
			TXJSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
			ok = file:write_file(TXFilepath, TXJSON)
		end,
		B0#block.txs
	),
	%% Write block index.
	BI = [ar_util:block_index_entry_from_block(B0)],
	BIJSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(BI)),
	HashListDir = filename:join(DataDir, ?HASH_LIST_DIR),
	BIFilepath = filename:join(HashListDir, <<"last_block_index.json">>),
	ok = file:write_file(BIFilepath, BIJSON),
	%% Write accounts.
	WalletListDir = filename:join(DataDir, ?WALLET_LIST_DIR),
	RootHash = B0#block.wallet_list,
	WalletListFilepath =
		filename:join(WalletListDir, binary_to_list(ar_util:encode(RootHash)) ++ ".json"),
	WalletListJSON =
		ar_serialize:jsonify(
			ar_serialize:wallet_list_to_json_struct(B0#block.reward_addr, false, WalletList)
		),
	ok = file:write_file(WalletListFilepath, WalletListJSON).
