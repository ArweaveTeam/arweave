-module(ar_device_mode).

-behaviour(gen_server).

-export([get_device_to_store_ids_map/0, get_store_id_to_device_map/0,
		get_store_ids_for_device/1]).

-export([start_link/0, init/1, handle_call/3, handle_info/2]).

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id_to_status = #{},
	device_to_store_ids = #{}
}).

-record(module_status, {
	device = undefined :: undefined | string(),
	prepare = undefined :: undefined | off | paused | complete | active,
	sync = undefined :: undefined | off | paused | active,
	repack = undefined :: undefined | off | paused | complete | active
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================
get_device_to_store_ids_map() ->
	State = gen_server:call(?MODULE, get_state),
	State#state.device_to_store_ids.

get_store_id_to_device_map() ->
	State = gen_server:call(?MODULE, get_state),
	StoreIDToStatus = State#state.store_id_to_status,
	% Transform StoreIDToStatus to map StoreIDs to devices
	maps:map(fun(_StoreID, #module_status{device = Device}) -> Device end, StoreIDToStatus).

get_store_ids_for_device(Device) ->
	DeviceToStoreIDs = get_device_to_store_ids_map(),
	maps:get(Device, DeviceToStoreIDs, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	[ok] = ar_events:subscribe([node_state]),

	%% Map devices to store ids.
	{ok, Config} = application:get_env(arweave, config),
	{ok, initialize_state(Config#config.storage_modules)}.
	

handle_call(get_state, _From, State) ->
	{reply, State, State};
handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_info({event, node_state, {initialized, _B}}, State) ->
	State3 = case ar_node:is_joined() of
		false ->
			State;
		true ->
			State2 = refresh_state(State),
			push_state(State2)
	end,
	{noreply, State3};
handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state(StorageModules) ->
	DeviceToStoreIDs = lists:foldl(
		fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
			Device = get_system_device(Module),
			maps:update_with(Device, fun(StoreIDs) -> [StoreID | StoreIDs] end, [StoreID], Acc)
		end,
		#{},
		StorageModules
	),

	StoreIDToStatus = maps:fold(
		fun(Device, StoreIDs, Acc) ->
			lists:foldl(
				fun(StoreID, Acc) -> 
					Status = #module_status{device = Device},
					maps:put(StoreID, Status, Acc)
				end,
				Acc, StoreIDs)
		end,
		#{},
		DeviceToStoreIDs
	),
	#state{
		device_to_store_ids = DeviceToStoreIDs,
		store_id_to_status = StoreIDToStatus
	}.

refresh_state(State) ->
	StoreIDToStatus = maps:map(
		fun(StoreID, Status) -> 
			Prepare = query_prepare_status(StoreID),
			Sync = query_sync_status(StoreID),
			Repack = query_repack_status(StoreID),
			Status#module_status{ prepare = Prepare, sync = Sync, repack = Repack }
		end,
		State#state.store_id_to_status
	),
	State#state{
		store_id_to_status = StoreIDToStatus
	}.

get_system_device(StorageModule) ->
	{ok, Config} = application:get_env(arweave, config),
	StoreID = ar_storage_module:id(StorageModule),
	Path = ar_chunk_storage:get_chunk_storage_path(Config#config.data_dir, StoreID),
	Device = ar_util:get_system_device(Path),
	case Device of
		"" -> StoreID;  % If the command fails or returns an empty string, return StoreID
		_ -> Device
	end.

query_prepare_status(StoreID) ->
	NeedsPrepare = ar_chunk_storage:needs_prepare(StoreID),
	IsPrepared = ar_chunk_storage:is_prepared(StoreID),
	case {NeedsPrepare, IsPrepared} of
		{true, true} -> complete;
		{true, false} -> paused;
		{false, _} -> off;
		Error ->
			?LOG_WARNING([
				{event, error_refreshing_state},
				{module, ?MODULE},
				{field, prepare},
				{store_id, StoreID},
				{error, Error}
			]),
			off
	end.

query_sync_status(StoreID) ->
	SyncingEnabled = ar_data_sync_worker_master:is_syncing_enabled(),
	IsSyncing = ar_data_sync:is_syncing(StoreID),
	case {SyncingEnabled, IsSyncing} of
		{false, _} -> off;
		{true, true} -> active;
		{true, false} -> paused;
		Error ->
			?LOG_WARNING([
				{event, error_refreshing_state},
				{module, ?MODULE},
				{field, sync},
				{store_id, StoreID},
				{error, Error}
			]),
			off
	end.

query_repack_status(StoreID) ->
	off.

push_state(State) ->
	DeviceStatuses = get_all_device_modes(2, State),
	State2 = enforce_device_modes(DeviceStatuses, State),
	State2.

get_all_device_modes(MaxPrepareModules, State) ->
	DeviceStatuses = maps:fold(
		fun(Device, StoreIDs, Acc) ->
			Status = get_device_mode(StoreIDs, State),
			CurrentDevices = maps:get(Status, Acc, []),
			UpdatedDevices = [Device | CurrentDevices],
			maps:put(Status, UpdatedDevices, Acc)
		end,
		#{},
		State#state.device_to_store_ids),

	PrepareDevices = maps:get(prepare, DeviceStatuses, []),
	SyncDevices = maps:get(sync, DeviceStatuses, []),

	% Adjust the number of devices in PrepareDevices and SyncDevices
	case length(PrepareDevices) of
		Length when Length > MaxPrepareModules ->
			% Move devices from PrepareDevices to SyncDevices
			{DevicesToMove, RemainingPrepareDevices} = lists:split(Length - MaxPrepareModules, PrepareDevices),
			UpdatedSyncDevices = DevicesToMove ++ SyncDevices,
			DeviceStatuses2 = maps:put(prepare, RemainingPrepareDevices, DeviceStatuses),
			maps:put(sync, UpdatedSyncDevices, DeviceStatuses2);
		Length when Length < MaxPrepareModules ->
			% Move devices from SyncDevices to PrepareDevices
			{DevicesToMove, RemainingSyncDevices} = lists:split(MaxPrepareModules - Length, SyncDevices),
			UpdatedPrepareDevices = DevicesToMove ++ PrepareDevices,
			DeviceStatuses2 = maps:put(prepare, UpdatedPrepareDevices, DeviceStatuses),
			maps:put(sync, RemainingSyncDevices, DeviceStatuses2);
		_ ->
			DeviceStatuses
	end.
	

get_device_mode(StoreIDs, State) ->
	StoreIDToStatus = State#state.store_id_to_status,
	IsPreparing = lists:any(
		fun(StoreID) ->
			case maps:get(StoreID, StoreIDToStatus) of
				#module_status{prepare = active} -> true;
				_ -> false
			end
		end, StoreIDs),
	IsRepacking = false,
	IsOff = lists:all(
		fun(StoreID) ->
			case maps:get(StoreID, StoreIDToStatus) of
				#module_status{prepare = off, sync = off, repack = off} -> true;
				_ -> false
			end
		end, StoreIDs),
	case {IsOff, IsPreparing, IsRepacking} of
		{true, _, _} -> off;
		{_, true, _} -> prepare;
		{_, _, true} -> repack;
		_ -> sync
	end.

enforce_device_modes(DeviceStatuses, State) ->
	maps:fold(
		fun(Status, Devices, Acc) ->
			case Status of
				prepare -> set_prepare_mode(Devices, Acc);
				repack -> set_repack_mode(Devices, Acc);
				sync -> set_sync_mode(Devices, Acc);
				off -> set_off_mode(Devices, Acc)
			end
		end,
		State,
		DeviceStatuses
	).

set_prepare_mode([Device | Devices], State) ->
	StoreIDs = maps:get(Device, State#state.device_to_store_ids, []),
	StoreIDToStatus = lists:foldl(
		fun(StoreID, {HasOnePrepareModule, Acc}) ->
			Status = maps:get(StoreID, Acc),
			NewPrepareStatus = case {HasOnePrepareModule, Status#module_status.prepare} of
				{true, PrepareStatus} ->
					pause_status(PrepareStatus);
				{false, paused} ->
					prepare;
				{_, PrepareStatus} ->
					PrepareStatus
			end,
			Status2 = Status#module_status{
				prepare = NewPrepareStatus,
				sync = pause_status(Status#module_status.sync),
				repack = pause_status(Status#module_status.repack)
			},
			{
				HasOnePrepareModule orelse NewPrepareStatus == prepare,
				maps:put(StoreID, Status2, Acc)
			}
		end,
		{false, State#state.store_id_to_status},
		StoreIDs
	),
	State2 = State#state{store_id_to_status = StoreIDToStatus},
	set_prepare_mode(Devices, State2);
set_prepare_mode([], State) ->
	State.

set_repack_mode([Device | Devices], State) ->
	set_repack_mode(Devices, State);
set_repack_mode([], State) ->
	State.

set_sync_mode([Device | Devices], State) ->
	StoreIDs = maps:get(Device, State#state.device_to_store_ids, []),
	StoreIDToStatus = lists:foldl(
		fun(StoreID, Acc) ->
			Status = maps:get(StoreID, State#state.store_id_to_status),
			Status2 = Status#module_status{
				prepare = pause_status(Status#module_status.prepare),
				sync = active_status(Status#module_status.sync),
				repack = pause_status(Status#module_status.repack)
			},
			maps:put(StoreID, Status2, Acc)
		end,
		State#state.store_id_to_status,
		StoreIDs
	),
	State2 = State#state{store_id_to_status = StoreIDToStatus},
	set_sync_mode(Devices, State2);
set_sync_mode([], State) ->
	State.

set_off_mode([Device | Devices], State) ->
	StoreIDs = maps:get(Device, State#state.device_to_store_ids, []),
	StoreIDToStatus = lists:foldl(
		fun(StoreID, Acc) ->
			Status = maps:get(StoreID, State#state.store_id_to_status),
			Status2 = Status#module_status{prepare = off, sync = off, repack = off},
			maps:put(StoreID, Status2, Acc)
		end,
		State#state.store_id_to_status,
		StoreIDs
	),
	State2 = State#state{store_id_to_status = StoreIDToStatus},
	set_off_mode(Devices, State2);
set_off_mode([], State) ->
	State.

pause_status(active) ->
	paused;
pause_status(Status) ->
	Status.

active_status(paused) ->
	active;
active_status(Status) ->
	Status.

%%%===================================================================
%%% Tests.
%%%===================================================================

refresh_state_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_util, get_system_device, fun mocked_get_system_device/1},
			{ar_chunk_storage, needs_prepare, fun mocked_needs_prepare/1},
			{ar_chunk_storage, is_prepared, fun mocked_is_prepared/1},
			{ar_data_sync_worker_master, is_syncing_enabled, fun mocked_is_syncing_enabled/0},
			{ar_data_sync, is_syncing, fun mocked_is_syncing/1}
		],
		fun test_refresh_state/0, 30)
	].

mocked_get_system_device(Path) ->
	Map = #{ 
		".tmp/data_test_main_localtest/storage_modules/storage_module_0_unpacked/chunk_storage"
			=> "device1",
		".tmp/data_test_main_localtest/storage_modules/storage_module_1_unpacked/chunk_storage"
			=> "device2",
		".tmp/data_test_main_localtest/storage_modules/storage_module_2_unpacked/chunk_storage"
			=> "device1",
		".tmp/data_test_main_localtest/storage_modules/storage_module_3_unpacked/chunk_storage"
			=> "device1"
	},
	maps:get(Path, Map).
mocked_needs_prepare(StoreID) ->
	Map = #{
		"storage_module_0_unpacked" => true,
		"storage_module_1_unpacked" => true,
		"storage_module_2_unpacked" => false,
		"storage_module_3_unpacked" => {error, timeout}
	},
	maps:get(StoreID, Map).
mocked_is_prepared(StoreID) ->
	Map = #{
		"storage_module_0_unpacked" => true,
		"storage_module_1_unpacked" => false,
		"storage_module_2_unpacked" => false,
		"storage_module_3_unpacked" => false
	},
	maps:get(StoreID, Map).

mocked_is_syncing_enabled() ->
	true.

mocked_is_syncing(StoreID) ->
	Map = #{
		"storage_module_0_unpacked" => {error, timeout},
		"storage_module_1_unpacked" => false,
		"storage_module_2_unpacked" => true,
		"storage_module_3_unpacked" => false
	},
	maps:get(StoreID, Map).

test_refresh_state() ->
	StorageModules = [
		{?PARTITION_SIZE, 0, unpacked},
		{?PARTITION_SIZE, 1, unpacked},
		{?PARTITION_SIZE, 2, unpacked},
		{?PARTITION_SIZE, 3, unpacked}
	],
	State1 = initialize_state(StorageModules),

	ExpectedDeviceToStoreIDs1 = #{
		"device1" => [
			"storage_module_3_unpacked",
			"storage_module_2_unpacked",
			"storage_module_0_unpacked"
		],
		"device2" => [
			"storage_module_1_unpacked"
		]
	},
	ExpectedStoreIDToStatus1 = #{
		"storage_module_0_unpacked" => #module_status{device = "device1"},
		"storage_module_1_unpacked" => #module_status{device = "device2"},
		"storage_module_2_unpacked" => #module_status{device = "device1"},
		"storage_module_3_unpacked" => #module_status{device = "device1"}
	},

	?assertEqual(ExpectedDeviceToStoreIDs1, State1#state.device_to_store_ids),
	?assertEqual(ExpectedStoreIDToStatus1, State1#state.store_id_to_status),

	State2 = refresh_state(State1),

	ExpectedStoreIDToStatus2 = #{
		"storage_module_0_unpacked" => #module_status{
			device = "device1", prepare = complete, sync = off, repack = off},
		"storage_module_1_unpacked" => #module_status{
			device = "device2", prepare = paused, sync = paused, repack = off},
		"storage_module_2_unpacked" => #module_status{
			device = "device1", prepare = off, sync = active, repack = off},
		"storage_module_3_unpacked" => #module_status{
			device = "device1", prepare = off, sync = paused, repack = off}
	},
	?assertEqual(ExpectedStoreIDToStatus2, State2#state.store_id_to_status).

device_statuses_test_() ->
	[
		{timeout, 30, fun test_basic_device_modes/0},
		{timeout, 30, fun test_rebalance_prepare_modes/0}
	].

test_basic_device_modes() ->
	State = #state{
		device_to_store_ids = #{
			"device1" => [
				"storage_module_0_unpacked",
				"storage_module_1_unpacked",
				"storage_module_2_unpacked"
			],
			"device2" => [
				"storage_module_3_unpacked",
				"storage_module_4_unpacked",
				"storage_module_5_unpacked"
			],
			"device3" => [
				"storage_module_6_unpacked",
				"storage_module_7_unpacked",
				"storage_module_8_unpacked"
			]
		},
		store_id_to_status = #{
			"storage_module_0_unpacked" => 
				#module_status{
					device = "device1",
					prepare = complete,
					sync = off,
					repack = off
				},
			"storage_module_1_unpacked" => 
				#module_status{
					device = "device1",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_2_unpacked" =>
				#module_status{
					device = "device1",
					prepare = active,
					sync = paused,
					repack = off
				},
			"storage_module_3_unpacked" => 
				#module_status{
					device = "device2",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_4_unpacked" => 
				#module_status{
					device = "device2",
					prepare = off,
					sync = paused,
					repack = off
				},
			"storage_module_5_unpacked" =>
				#module_status{
					device = "device2",
					prepare = complete,
					sync = active,
					repack = off
				},
			"storage_module_6_unpacked" => 
				#module_status{
					device = "device3",
					prepare = off,
					sync = off,
					repack = off
				},
			"storage_module_7_unpacked" => 
				#module_status{
					device = "device3",
					prepare = off,
					sync = off,
					repack = off
				},
			"storage_module_8_unpacked" =>
				#module_status{
					device = "device3",
					prepare = off,
					sync = off,
					repack = off
				}
		}
	},
	ExpectedDeviceModes = #{
		prepare => ["device1"],
		sync => ["device2"],
		off => ["device3"]
	},
	?assertEqual(ExpectedDeviceModes, get_all_device_modes(1, State)).


test_rebalance_prepare_modes() ->
	State = #state{
		device_to_store_ids = #{
			"device1" => [
				"storage_module_0_unpacked",
				"storage_module_1_unpacked",
				"storage_module_2_unpacked"
			],
			"device2" => [
				"storage_module_3_unpacked",
				"storage_module_4_unpacked",
				"storage_module_5_unpacked"
			],
			"device3" => [
				"storage_module_6_unpacked",
				"storage_module_7_unpacked",
				"storage_module_8_unpacked"
			],
			"device4" => [
				"storage_module_9_unpacked",
				"storage_module_10_unpacked",
				"storage_module_11_unpacked"
			]
		},
		store_id_to_status = #{
			"storage_module_0_unpacked" => 
				#module_status{
					device = "device1",
					prepare = complete,
					sync = paused,
					repack = off
				},
			"storage_module_1_unpacked" => 
				#module_status{
					device = "device1",
					prepare = paused,
					sync = paused,
					repack = off
				},
			"storage_module_2_unpacked" =>
				#module_status{
					device = "device1",
					prepare = active,
					sync = paused,
					repack = off
				},
			"storage_module_3_unpacked" => 
				#module_status{
					device = "device2",
					prepare = active,
					sync = paused,
					repack = off
				},
			"storage_module_4_unpacked" => 
				#module_status{
					device = "device2",
					prepare = active,
					sync = paused,
					repack = off
				},
			"storage_module_5_unpacked" =>
				#module_status{
					device = "device2",
					prepare = complete,
					sync = paused,
					repack = off
				},
			"storage_module_6_unpacked" => 
				#module_status{
					device = "device3",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_7_unpacked" => 
				#module_status{
					device = "device3",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_8_unpacked" =>
				#module_status{
					device = "device3",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_9_unpacked" =>
				#module_status{
					device = "device4",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_10_unpacked" =>
				#module_status{
					device = "device4",
					prepare = paused,
					sync = active,
					repack = off
				},
			"storage_module_11_unpacked" =>
				#module_status{
					device = "device4",
					prepare = paused,
					sync = active,
					repack = off
				}
		}
	},
	ExpectedDeviceModes1 = #{
		prepare => ["device2", "device1"],
		sync => ["device4", "device3"]
	},
	?assertEqual(ExpectedDeviceModes1, get_all_device_modes(2, State)),
	ExpectedDeviceModes2 = #{
		prepare => ["device4", "device2", "device1"],
		sync => ["device3"]
	},
	?assertEqual(ExpectedDeviceModes2, get_all_device_modes(3, State)),
	ExpectedDeviceModes3 = #{
		prepare => ["device4", "device3", "device2", "device1"],
		sync => []
	},
	?assertEqual(ExpectedDeviceModes3, get_all_device_modes(4, State)),
	ExpectedDeviceModes4 = #{
		prepare => ["device1"],
		sync => ["device2", "device4", "device3"]
	},
	?assertEqual(ExpectedDeviceModes4, get_all_device_modes(1, State)),
	ExpectedDeviceModes5 = #{
		prepare => [],
		sync => ["device2", "device1", "device4", "device3"]
	},
	?assertEqual(ExpectedDeviceModes5, get_all_device_modes(0, State)).


	
