-module(ar_p3_ledger_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(with_setup(CheckpointInterval, ShutdownTimeoutS, Test), parametric_setup(CheckpointInterval, ShutdownTimeoutS, Test)).
-define(with_setup(Test), ?with_setup(100, 60, Test)).
-define(timeout(Timeout, TestOrTests), {timeout, Timeout, TestOrTests}).

-define(peer_address, <<"PEERADDRESS">>).
-define(asset0, <<"test0/T0">>).
-define(asset1, <<"test1/T1">>).



%%% ==================================================================
%%% Utility.
%%% ==================================================================



parametric_setup(CheckpointInterval, ShutdownTimeoutS, Tests) ->
  {ok, OriginalConfig} = application:get_env(arweave, config),
  AlteredConfig = OriginalConfig#config{
    p3_ledger_checkpoint_interval = CheckpointInterval,
    p3_ledger_shutdown_timeout = ShutdownTimeoutS
  },
  {setup, fun() ->
    ok = application:set_env(arweave, config, AlteredConfig, [{persistent, true}]),
    ok = supervisor:terminate_child(ar_sup, ar_p3_sup),
    {ok, _Pid} = supervisor:restart_child(ar_sup, ar_p3_sup),
    test_setup()
  end, fun(_) ->
    test_teardown(),
    ok = application:set_env(arweave, config, OriginalConfig, [{persistent, true}])
  end, [Tests]}.



test_setup() ->

  ok = maybe_delete_rocksdb_database(?peer_address),
  ok.



test_teardown() ->
  ok = maybe_terminate_ledger_gracefully(?peer_address),
  ok.



maybe_terminate_ledger_gracefully(PeerAddress) ->
  MaybeProcessPid = gproc:where(ar_p3_ledger:via_reg_name(PeerAddress)),
  case MaybeProcessPid of
    undefined -> ok;
    Pid -> supervisor:terminate_child(ar_p3_sup, Pid)
  end,
  ok.



maybe_terminate_ledger_brutally(PeerAddress) ->
  MaybeProcessPid = gproc:where(ar_p3_ledger:via_reg_name(PeerAddress)),
  case MaybeProcessPid of
    undefined -> ok;
    Pid -> erlang:exit(Pid, kill)
  end,
  ok.



maybe_delete_rocksdb_database(PeerAddress) ->
  DbDir = ar_p3_ledger:db_dir(PeerAddress),
  case file:del_dir_r(DbDir) of
    ok -> ok;
    {error, enoent} -> ok;
    {error, Reason} -> throw({error, {delete_database, Reason}})
  end,
  ok.



%%% ==================================================================
%%% Timing tests.
%%% ==================================================================



timing_test_() -> [?timeout(5000, [
  ?with_setup(100, 1, fun shutdown_timeout/0)
])].



shutdown_timeout() ->
  %% start the ledger with a transaction
  {ok, _} = ar_p3_ledger:deposit(?peer_address, ?asset0, 1000, #{}),
  Pid = gproc:where(ar_p3_ledger:via_reg_name(?peer_address)),

  timer:sleep(1000),
  ?assertMatch(true, erlang:is_process_alive(Pid), "ledger process is still alive"),
  timer:sleep(1000),
  ?assertMatch(false, erlang:is_process_alive(Pid), "ledger process is dead").



%%% ==================================================================
%%% Functionality tests.
%%% ==================================================================




ledger_functionality_test_() -> ?timeout(200, [
  ?with_setup(fun basic_deposit/0),
  ?with_setup(fun multi_asset_deposit/0),
  ?with_setup(fun basic_service/0),
  ?with_setup(fun multi_asset_service/0)
]).



basic_deposit() ->
  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:deposit(?peer_address, ?asset0, 1000, #{}),
    "deposit successful"
  ),
  ?assertMatch({ok, #{?asset0 := 1000}}, ar_p3_ledger:total_tokens(?peer_address), "tokens balance is correct"),
  ?assertMatch({ok, #{?asset0 := -1000}}, ar_p3_ledger:total_credits(?peer_address), "credits balance is correct"),
  ?assertMatch({ok, #{}}, ar_p3_ledger:total_services(?peer_address), "services balance is correct").



multi_asset_deposit() ->
  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:deposit(?peer_address, ?asset0, 1000, #{}),
    "deposit successful"
  ),
  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:deposit(?peer_address, ?asset1, 500, #{}),
    "deposit successful"
  ),
  ?assertMatch({ok, #{?asset0 := 1000, ?asset1 := 500}}, ar_p3_ledger:total_tokens(?peer_address), "tokens balance is correct"),
  ?assertMatch({ok, #{?asset0 := -1000, ?asset1 := -500}}, ar_p3_ledger:total_credits(?peer_address), "credits balance is correct"),
  ?assertMatch({ok, #{}}, ar_p3_ledger:total_services(?peer_address), "services balance is correct").



basic_service() ->
  {ok, _} = ar_p3_ledger:deposit(?peer_address, ?asset0, 1000, #{}),

  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:service(?peer_address, ?asset0, 100, #{}),
    "service successful"
  ),
  ?assertMatch({ok, #{?asset0 := 1000}}, ar_p3_ledger:total_tokens(?peer_address), "tokens balance is correct"),
  ?assertMatch({ok, #{?asset0 := -900}}, ar_p3_ledger:total_credits(?peer_address), "credits balance is correct"),
  ?assertMatch({ok, #{?asset0 := -100}}, ar_p3_ledger:total_services(?peer_address), "services balance is correct").



multi_asset_service() ->
  {ok, _} = ar_p3_ledger:deposit(?peer_address, ?asset0, 1000, #{}),
  {ok, _} = ar_p3_ledger:deposit(?peer_address, ?asset1, 500, #{}),

  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:service(?peer_address, ?asset0, 100, #{}),
    "service successful"
  ),
  ?assertMatch(
    {ok, Id} when is_binary(Id),
    ar_p3_ledger:service(?peer_address, ?asset1, 100, #{}),
    "service successful"
  ),
  ?assertMatch({ok, #{?asset0 := 1000, ?asset1 := 500}}, ar_p3_ledger:total_tokens(?peer_address), "tokens balance is correct"),
  ?assertMatch({ok, #{?asset0 := -900, ?asset1 := -400}}, ar_p3_ledger:total_credits(?peer_address), "credits balance is correct"),
  ?assertMatch({ok, #{?asset0 := -100, ?asset1 := -100}}, ar_p3_ledger:total_services(?peer_address), "services balance is correct").
