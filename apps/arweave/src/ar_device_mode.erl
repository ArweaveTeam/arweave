-module(ar_device_mode).

-behaviour(gen_server).

-export([get_device_to_store_ids_map/0, get_store_id_to_device_map/0,
		get_store_ids_for_device/1, acquire_lock/3, release_lock/2]).

-export([start_link/0, init/1, handle_call/3, handle_info/2, handle_cast/2]).

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id_to_device = #{},
	device_locks = #{}
}).

-type device_mode() :: prepare | sync | repack.

%%%===================================================================
%%% Public interface.
%%%===================================================================
get_device_to_store_ids_map() ->
	State = gen_server:call(?MODULE, get_state),
	ar_util:invert_map(State#state.store_id_to_device).

get_store_id_to_device_map() ->
	State = gen_server:call(?MODULE, get_state),
	State#state.store_id_to_device.

get_store_ids_for_device(Device) ->
	DeviceToStoreIDs = get_device_to_store_ids_map(),
	maps:get(Device, DeviceToStoreIDs, sets:new()).

%% @doc Helper function to wrap common logic around acquiring a device lock.
-spec acquire_lock(device_mode(), string(), atom()) -> atom().
acquire_lock(Mode, StoreID, CurrentStatus) ->
	case CurrentStatus of
		paused ->
			case gen_server:call(?MODULE, {acquire_lock, Mode, StoreID}) of
				true ->
					active;
				false ->
					CurrentStatus
			end;
		_ ->
			%% We already have the lock (e.g. 'active'),
			%% or don't really want it (e.g. 'complete', or 'off').
			CurrentStatus
	end.

release_lock(Mode, StoreID) ->
	gen_server:cast(?MODULE, {release_lock, Mode, StoreID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	{ok, initialize_state(Config#config.storage_modules)}.

handle_call(get_state, _From, State) ->
	{reply, State, State};
handle_call({acquire_lock, Mode, StoreID}, _From, State) ->
	{Acquired, State2} = do_acquire_lock(Mode, StoreID, State),
	{reply, Acquired, State2};
handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.


handle_cast({release_lock, Mode, StoreID}, State) ->
	State2 = do_release_lock(Mode, StoreID, State),
	{noreply, State2};
handle_cast(Request, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state(StorageModules) ->
	StoreIDToDevice = lists:foldl(
		fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
			Device = get_system_device(Module),
			maps:put(StoreID, Device, Acc)
		end,
		#{},
		StorageModules
	),

	#state{
		store_id_to_device = StoreIDToDevice
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

do_acquire_lock(Mode, StoreID, State) ->
	Device = maps:get(StoreID, State#state.store_id_to_device),
	DeviceLock = maps:get(Device, State#state.device_locks, sync),

	{Acquired, NewDeviceLock} = case Mode of
		sync ->
			%% Can only aquire a sync lock if the device is in sync mode
			case DeviceLock of
				sync -> {true, sync};
				_ -> {false, DeviceLock}
			end;
		prepare ->
			%% Can only acquire a prepare lock if the device is in sync mode or this
			%% StoreID already has the prepare lock
			case DeviceLock of
				sync -> {true, {prepare, StoreID}};
				{prepare, StoreID} -> {true, DeviceLock};
				_ -> {false, DeviceLock}
			end;
		repack ->
			%% Can only acquire a repack lock if the device is in sync mode or this
			%% StoreID already has the repack lock
			case DeviceLock of
				sync -> {true, {repack, StoreID}};
				{repack, StoreID} -> {true, DeviceLock};
				_ -> {false, DeviceLock}
			end
	end,

	DeviceLocks = maps:put(Device, NewDeviceLock, State#state.device_locks),
	{Acquired, State#state{device_locks = DeviceLocks}}.

do_release_lock(Mode, StoreID, State) ->
	Device = maps:get(StoreID, State#state.store_id_to_device),
	DeviceLock = maps:get(Device, State#state.device_locks, sync),
	NewDeviceLock = case Mode of
		sync ->
			%% Releasing a sync lock does nothing.
			DeviceLock;
		prepare ->
			case DeviceLock of
				{prepare, StoreID} ->
					%% This StoreID had a prepare lock on this device, so now we can
					%% put the device back in sync mode so it's ready to be locked again
					%% if needed.
					sync;
				_ ->
					%% We should only be able to release a prepare lock if we previously
					%% held it. If we hit this branch something is wrong.
					?LOG_WARNING([{event, invalid_release_lock},
							{module, ?MODULE}, {mode, Mode}, {store_id, StoreID},
							{current_lock, DeviceLock}]),
					DeviceLock
			end;
		repack ->
			case DeviceLock of
				{repack, StoreID} ->
					%% This StoreID had a repack lock on this device, so now we can
					%% put the device back in sync mode so it's ready to be locked again
					%% if needed.
					sync;
				_ ->
					%% We should only be able to release a repack lock if we previously
					%% held it. If we hit this branch something is wrong.
					?LOG_WARNING([{event, invalid_release_lock},
							{module, ?MODULE}, {mode, Mode}, {store_id, StoreID},
							{current_lock, DeviceLock}]),
					DeviceLock
			end
	end,

	DeviceLocks = maps:put(Device, NewDeviceLock, State#state.device_locks),
	State#state{device_locks = DeviceLocks}.

%%%===================================================================
%%% Tests.
%%%===================================================================
device_locks_test_() ->
	[
		{timeout, 30, fun test_acquire_lock/0},
		{timeout, 30, fun test_release_lock/0}
	].

test_acquire_lock() ->
	State = #state{
		store_id_to_device = #{
			"storage_module_0_unpacked" => "device1",
			"storage_module_1_unpacked" => "device1",
			"storage_module_2_unpacked" => "device2",
			"storage_module_3_unpacked" => "device2",
			"storage_module_4_unpacked" => "device3",
			"storage_module_5_unpacked" => "device3"
		},
		device_locks = #{
			"device1" => sync,
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => {repack, "storage_module_4_unpacked"}
		}
	},

	?assertEqual(
		{true, State}, 
		do_acquire_lock(sync, "storage_module_0_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(sync, "storage_module_2_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(sync, "storage_module_3_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(sync, "storage_module_4_unpacked", State)),

	
	?assertEqual(
		{true, State#state{device_locks = #{
			"device1" => {prepare, "storage_module_0_unpacked"},
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => {repack, "storage_module_4_unpacked"}
		}}}, 
		do_acquire_lock(prepare, "storage_module_0_unpacked", State)),
	?assertEqual(
		{true, State}, 
		do_acquire_lock(prepare, "storage_module_2_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(prepare, "storage_module_3_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(prepare, "storage_module_4_unpacked", State)),
	
	?assertEqual(
		{true, State#state{device_locks = #{
			"device1" => {repack, "storage_module_0_unpacked"},
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => {repack, "storage_module_4_unpacked"}
		}}}, 
		do_acquire_lock(repack, "storage_module_0_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(repack, "storage_module_2_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(repack, "storage_module_3_unpacked", State)),
	?assertEqual(
		{true, State}, 
		do_acquire_lock(repack, "storage_module_4_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(repack, "storage_module_5_unpacked", State)).

test_release_lock() ->
	State = #state{
		store_id_to_device = #{
			"storage_module_0_unpacked" => "device1",
			"storage_module_1_unpacked" => "device1",
			"storage_module_2_unpacked" => "device2",
			"storage_module_3_unpacked" => "device2",
			"storage_module_4_unpacked" => "device3",
			"storage_module_5_unpacked" => "device3"
		},
		device_locks = #{
			"device1" => sync,
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => {repack, "storage_module_4_unpacked"}
		}
	},

	?assertEqual(
		State, 
		do_release_lock(sync, "storage_module_0_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(sync, "storage_module_2_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(sync, "storage_module_3_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(sync, "storage_module_4_unpacked", State)),

	?assertEqual(
		State, 
		do_release_lock(prepare, "storage_module_0_unpacked", State)),
	?assertEqual(
		State#state{device_locks = #{
			"device1" => sync,
			"device2" => sync,
			"device3" => {repack, "storage_module_4_unpacked"}
		}}, 
		do_release_lock(prepare, "storage_module_2_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(prepare, "storage_module_3_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(prepare, "storage_module_4_unpacked", State)),

	?assertEqual(
		State, 
		do_release_lock(repack, "storage_module_0_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(repack, "storage_module_2_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(repack, "storage_module_3_unpacked", State)),
	?assertEqual(
		State#state{device_locks = #{
			"device1" => sync,
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => sync
		}}, 
		do_release_lock(repack, "storage_module_4_unpacked", State)),
	?assertEqual(
		State, 
		do_release_lock(repack, "storage_module_5_unpacked", State)).
