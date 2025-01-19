-module(ar_device_lock).

-behaviour(gen_server).

-export([get_store_id_to_device_map/0, is_ready/0, acquire_lock/3, release_lock/2]).

-export([start_link/0, init/1, handle_call/3, handle_info/2, handle_cast/2]).

-include("../include/ar.hrl").
-include("../include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id_to_device = #{},
	device_locks = #{},
	initialized = false,
	num_replica_2_9_workers = 0
}).

-type device_mode() :: prepare | sync | repack.

-define(DEVICE_LOCK_LOG_INTERVAL_MS, 600_000). %% 10 minutes

%%%===================================================================
%%% Public interface.
%%%===================================================================

get_store_id_to_device_map() ->
	case catch gen_server:call(?MODULE, get_state) of
		{'EXIT', {Reason, {gen_server, call, _}}} ->
			{error, Reason};
		State ->
			State#state.store_id_to_device
	end.

is_ready() ->
	case catch gen_server:call(?MODULE, get_state) of
		{'EXIT', {Reason, {gen_server, call, _}}} ->
			?LOG_WARNING([{event, error_getting_device_lock_state},
					{module, ?MODULE}, {reason, Reason}]),
			false;
		State ->
			State#state.initialized
	end.

%% @doc Helper function to wrap common logic around acquiring a device lock.
-spec acquire_lock(device_mode(), string(), atom()) -> atom().
acquire_lock(Mode, StoreID, CurrentStatus) ->
	NewStatus = case CurrentStatus of
		_ when CurrentStatus == complete; CurrentStatus == off ->
			% No change needed when we're done or off.
			CurrentStatus;
		_ ->
			case catch gen_server:call(?MODULE, {acquire_lock, Mode, StoreID}) of
				{'EXIT', {Reason, {gen_server, call, _}}} ->
					?LOG_WARNING([{event, error_acquiring_device_lock},
							{module, ?MODULE}, {reason, Reason}]),
					CurrentStatus;
				true ->
					active;
				false ->
					paused
			end
	end,

	case NewStatus == CurrentStatus of
		true ->
			ok;
		false ->
			?LOG_INFO([{event, acquire_device_lock}, {mode, Mode}, {store_id, StoreID},
					{old_status, CurrentStatus}, {new_status, NewStatus}])
	end,
	NewStatus.

release_lock(Mode, StoreID) ->
	gen_server:cast(?MODULE, {release_lock, Mode, StoreID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	gen_server:cast(self(), initialize_state),
	{ok, Config} = application:get_env(arweave, config),
	?LOG_INFO([{event, starting_device_lock_server},
		{num_replica_2_9_workers, Config#config.replica_2_9_workers}]),
	{ok, #state{num_replica_2_9_workers = Config#config.replica_2_9_workers}}.

handle_call(get_state, _From, State) ->
	{reply, State, State};
handle_call({acquire_lock, Mode, StoreID}, _From, State) ->
	case State#state.initialized of
		false ->
			% Not yet initialized.
			{reply, false, State};
		_ ->
			{Acquired, State2} = do_acquire_lock(Mode, StoreID, State),
			{reply, Acquired, State2}
	end;
handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(initialize_state, State) ->
	State2 = case ar_node:is_joined() of
		false ->
			ar_util:cast_after(1000, self(), initialize_state),
			State;
		true ->
			initialize_state(State)
	end,
	{noreply, State2};
handle_cast({release_lock, Mode, StoreID}, State) ->
	case State#state.initialized of
		false ->
			% Not yet initialized.
			{noreply, State};
		_ ->
			State2 = do_release_lock(Mode, StoreID, State),
			?LOG_INFO([{event, release_device_lock}, {mode, Mode}, {store_id, StoreID}]),
			{noreply, State2}
	end;
handle_cast(log_device_locks, State) ->
	log_device_locks(State),
	ar_util:cast_after(?DEVICE_LOCK_LOG_INTERVAL_MS, ?MODULE, log_device_locks), 
	{noreply, State};
handle_cast(Request, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.


handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state(State) ->
	{ok, Config} = application:get_env(arweave, config),
	StorageModules = Config#config.storage_modules,
	StoreIDToDevice = lists:foldl(
		fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
			Device = get_system_device(Module),
			?LOG_INFO([
				{event, storage_module_device}, {store_id, StoreID}, {device, Device}]),
			maps:put(StoreID, Device, Acc)
		end,
		#{},
		StorageModules
	),

	log_device_locks(State),
	ar_util:cast_after(?DEVICE_LOCK_LOG_INTERVAL_MS, ?MODULE, log_device_locks), 

	State#state{
		store_id_to_device = StoreIDToDevice,
		initialized = true
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
	MaxPrepareLocks = State#state.num_replica_2_9_workers,
	Device = maps:get(StoreID, State#state.store_id_to_device),
	DeviceLock = maps:get(Device, State#state.device_locks, sync),
	PrepareLocks = count_prepare_locks(State),
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
			case {DeviceLock, PrepareLocks} of
				{sync, _} when PrepareLocks < MaxPrepareLocks -> {true, {prepare, StoreID}};
				{{prepare, StoreID}, _} -> {true, DeviceLock};
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

count_prepare_locks(State) ->
	maps:fold(
		fun(_Device, Lock, Acc) ->
			case Lock of
				{prepare, _} -> Acc + 1;
				_ -> Acc
			end
		end,
		0,
		State#state.device_locks
	).

log_device_locks(State) ->
	StoreIDToDevice = State#state.store_id_to_device,
	DeviceLocks = State#state.device_locks,
	SortedStoreIDList = lists:sort(
		fun({StoreID1, Device1}, {StoreID2, Device2}) ->
			case Device1 =:= Device2 of
				true -> StoreID1 =< StoreID2;
				false -> Device1 < Device2
			end
		end,
		maps:to_list(StoreIDToDevice)),
	lists:foreach(
		fun({StoreID, Device}) ->
			DeviceLock = maps:get(Device, DeviceLocks, sync),
			Status = case DeviceLock of
				sync -> sync;
				{prepare, StoreID} -> prepare;
				{repack, StoreID} -> repack;
				_ -> paused
			end,
			?LOG_INFO([{event, device_lock_status}, {device, Device}, {store_id, StoreID}, {status, Status}])
		end,
		SortedStoreIDList
	).

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
