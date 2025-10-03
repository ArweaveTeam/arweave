-module(ar_device_lock).

-behaviour(gen_server).

-export([get_store_id_to_device_map/0, is_ready/0, acquire_lock/3, release_lock/2,
	set_device_lock_metric/3]).

-export([start_link/0, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id_to_device = #{},
	device_locks = #{}, %% used when device_limit is true
	store_id_locks = #{}, %% used when device_limit is false
	initialized = false,
	num_replica_2_9_workers = 0,
	device_limit = true
}).

-type device_mode() :: prepare | sync | repack.

-ifdef(AR_TEST).
-define(LOCK_LOG_INTERVAL_MS, 10_000). %% 10 seconds
-else.
-define(LOCK_LOG_INTERVAL_MS, 600_000). %% 10 minutes
-endif.

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
			set_device_lock_metric(StoreID, Mode, NewStatus),
			?LOG_INFO([{event, acquire_device_lock}, {mode, Mode}, {store_id, StoreID},
					{old_status, CurrentStatus}, {new_status, NewStatus}])
	end,
	NewStatus.

release_lock(Mode, StoreID) ->
	gen_server:cast(?MODULE, {release_lock, Mode, StoreID}).

set_device_lock_metric(StoreID, Mode, Status) ->
	StatusCode = case Status of
		off -> -1;
		paused -> 0;
		active -> 1;
		complete -> 2;
		_ -> -2		
	end,
	StoreIDLabel = ar_storage_module:label(StoreID),
	prometheus_gauge:set(device_lock_status, [StoreIDLabel, Mode], StatusCode).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	gen_server:cast(self(), initialize_state),
	{ok, Config} = arweave_config:get_env(),

	State = #state{
		num_replica_2_9_workers = Config#config.replica_2_9_workers,
		device_limit = not Config#config.disable_replica_2_9_device_limit
	},
	?LOG_INFO([{event, starting_device_lock_server},
		{num_replica_2_9_workers, State#state.num_replica_2_9_workers},
		{device_limit, State#state.device_limit}]),
	{ok, State}.

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
handle_cast(log_locks, State) ->
	log_locks(State),
	ar_util:cast_after(?LOCK_LOG_INTERVAL_MS, ?MODULE, log_locks), 
	{noreply, State};
handle_cast(Request, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.


handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state(State) ->
	{ok, Config} = arweave_config:get_env(),
	StorageModules = Config#config.storage_modules,
	RepackInPlaceModules = [element(1, El)
			|| El <- Config#config.repack_in_place_storage_modules],
	StoreIDToDevice = lists:foldl(
		fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
			Device = get_system_device(Module),
			?LOG_INFO([
				{event, storage_module_device}, {store_id, StoreID}, {device, Device}]),
			maps:put(StoreID, Device, Acc)
		end,
		#{},
		StorageModules ++ RepackInPlaceModules
	),
	State2 = State#state{
		store_id_to_device = StoreIDToDevice,
		initialized = true
	},
	
	log_locks(State2),
	ar_util:cast_after(?LOCK_LOG_INTERVAL_MS, ?MODULE, log_locks), 

	State2.

get_system_device(StorageModule) ->
	{ok, Config} = arweave_config:get_env(),
	StoreID = ar_storage_module:id(StorageModule),
	Path = ar_chunk_storage:get_chunk_storage_path(Config#config.data_dir, StoreID),
	Device = ar_util:get_system_device(Path),
	case Device of
		"" -> StoreID;  % If the command fails or returns an empty string, return StoreID
		_ -> Device
	end.

do_acquire_lock(Mode, ?DEFAULT_MODULE, State) ->
	%% "default" storage module is a special case. It can only be in sync mode.
	case Mode of
		sync ->
			{true, State};
		_ ->
			{false, State}
	end;
do_acquire_lock(Mode, StoreID, State) ->
	MaxPrepareLocks = State#state.num_replica_2_9_workers,
	Lock = query_lock(StoreID, State),
	PrepareLocks = count_prepare_locks(State),
	{Acquired, NewLock} = case Mode of
		sync ->
			%% Can only aquire a sync lock if the device is in sync mode
			case Lock of
				sync -> {true, sync};
				_ -> {false, Lock}
			end;
		prepare ->
			%% Can only acquire a prepare lock if the device is in sync mode or this
			%% StoreID already has the prepare lock
			case {Lock, PrepareLocks} of
				{sync, _} when PrepareLocks < MaxPrepareLocks -> {true, prepare};
				{prepare, _} -> {true, Lock};
				_ -> {false, Lock}
			end;
		repack ->
			%% Can only acquire a repack lock if the device is in sync mode or this
			%% StoreID already has the repack lock
			case {Lock, PrepareLocks} of
				{sync, _} when PrepareLocks < MaxPrepareLocks -> {true, repack};
				{repack, _} -> {true, Lock};
				_ -> {false, Lock}
			end
	end,
	{Acquired, update_lock(StoreID, NewLock, State)}.

do_release_lock(Mode, StoreID, State) ->
	Lock = query_lock(StoreID, State),
	NewLock = case Mode of
		sync ->
			%% Releasing a sync lock does nothing.
			Lock;
		prepare ->
			case Lock of
				prepare ->
					%% This StoreID had a prepare lock on this device, so now we can
					%% put the device back in sync mode so it's ready to be locked again
					%% if needed.
					sync;
				_ ->
					%% We should only be able to release a prepare lock if we previously
					%% held it. If we hit this branch something is wrong.
					?LOG_WARNING([{event, invalid_release_lock},
							{module, ?MODULE}, {mode, Mode}, {store_id, StoreID},
							{current_lock, Lock}]),
					Lock
			end;
		repack ->
			case Lock of
				repack ->
					%% This StoreID had a repack lock on this device, so now we can
					%% put the device back in sync mode so it's ready to be locked again
					%% if needed.
					sync;
				_ ->
					%% We should only be able to release a repack lock if we previously
					%% held it. If we hit this branch something is wrong.
					?LOG_WARNING([{event, invalid_release_lock},
							{module, ?MODULE}, {mode, Mode}, {store_id, StoreID},
							{current_lock, Lock}]),
					Lock
			end
	end,

	update_lock(StoreID, NewLock, State).

count_prepare_locks(#state{ device_limit = true } = State) ->
	maps:fold(
		fun(_Device, {prepare, _}, Acc) -> Acc + 1;
		   (_Device, _, Acc) -> Acc
		end,
		0,
		State#state.device_locks
	);
count_prepare_locks(#state{ device_limit = false } = State) ->
	maps:fold(
		fun(_Device, prepare, Acc) -> Acc + 1;
		   (_Device, _, Acc) -> Acc
		end,
		0,
		State#state.store_id_locks
	).

query_lock(StoreID, #state{ device_limit = true } = State) ->
	Device = maps:get(StoreID, State#state.store_id_to_device),
	DeviceLock = maps:get(Device, State#state.device_locks, sync),
	case DeviceLock of
		sync -> sync;
		{prepare, StoreID} -> prepare;
		{repack, StoreID} -> repack;
		_ -> paused
	end;
query_lock(StoreID, #state{ device_limit = false } = State) ->
	maps:get(StoreID, State#state.store_id_locks, sync).

update_lock(StoreID, Lock, #state{ device_limit = true } = State) ->
	Device = maps:get(StoreID, State#state.store_id_to_device),
	DeviceLocks = case Lock of
		paused -> State#state.device_locks;
		sync -> maps:put(Device, sync, State#state.device_locks);
		prepare -> maps:put(Device, {prepare, StoreID}, State#state.device_locks);
		repack -> maps:put(Device, {repack, StoreID}, State#state.device_locks)
	end,
	State#state{device_locks = DeviceLocks};
update_lock(StoreID, Lock, #state{ device_limit = false } = State) ->
	StoreIDLocks = maps:put(StoreID, Lock, State#state.store_id_locks),
	State#state{store_id_locks = StoreIDLocks}.

log_locks(State) ->
	Logs = do_log_locks(State),
	lists:foreach(fun(Log) -> ?LOG_INFO(Log) end, Logs).

do_log_locks(#state{ device_limit = true } = State) ->
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
	lists:foldr(
		fun({StoreID, Device}, Acc) ->
			DeviceLock = maps:get(Device, DeviceLocks, sync),
			Status = case DeviceLock of
				sync -> sync;
				{prepare, StoreID} -> prepare;
				{repack, StoreID} -> repack;
				_ -> paused
			end,
			[
				[{event, device_lock_status}, {device, Device}, {store_id, StoreID}, {status, Status}] 
				| Acc
			]
		end,
		[],
		SortedStoreIDList
	);
do_log_locks(#state{ device_limit = false } = State) ->
	StoreIDToDevice = State#state.store_id_to_device,
	StoreIDLocks = State#state.store_id_locks,
	SortedStoreIDList = lists:sort(
		fun({StoreID1, Device1}, {StoreID2, Device2}) ->
			case Device1 =:= Device2 of
				true -> StoreID1 =< StoreID2;
				false -> Device1 < Device2
			end
		end,
		maps:to_list(StoreIDToDevice)),
	lists:foldr(
		fun({StoreID, Device}, Acc) ->
			Status = maps:get(StoreID, StoreIDLocks, sync),
			[
				[{event, device_lock_status}, {device, Device}, {store_id, StoreID}, {status, Status}] 
				| Acc
			]
		end,
		[],
		SortedStoreIDList
	).

%%%===================================================================
%%% Tests.
%%%===================================================================
device_locks_test_() ->
	[
		{timeout, 30, fun test_acquire_lock/0},
		{timeout, 30, fun test_acquire_lock_without_device_limit/0},
		{timeout, 30, fun test_release_lock/0},
		{timeout, 30, fun test_release_lock_without_device_limit/0},
		{timeout, 30, fun test_count_prepare_locks/0},
		{timeout, 30, fun test_log_locks/0}
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
		},
		num_replica_2_9_workers = 2
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
		{false, State#state{num_replica_2_9_workers = 1}},
		do_acquire_lock(prepare, "storage_module_0_unpacked",
			State#state{num_replica_2_9_workers = 1})),
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

test_acquire_lock_without_device_limit() ->
	State = #state{
		store_id_to_device = #{
			"storage_module_0_unpacked" => "device1",
			"storage_module_1_unpacked" => "device1",
			"storage_module_2_unpacked" => "device2",
			"storage_module_3_unpacked" => "device2",
			"storage_module_4_unpacked" => "device3",
			"storage_module_5_unpacked" => "device3"
		},
		store_id_locks = #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		},
		num_replica_2_9_workers = 3,
		device_limit = false
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
		{false, State#state{num_replica_2_9_workers = 2}},
		do_acquire_lock(prepare, "storage_module_0_unpacked",
			State#state{num_replica_2_9_workers = 2})),
	?assertEqual(
		{true, State#state{store_id_locks = #{
			"storage_module_0_unpacked" => prepare,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		}}},
		do_acquire_lock(prepare, "storage_module_0_unpacked", State)),
	?assertEqual(
		{true, State}, 
		do_acquire_lock(prepare, "storage_module_2_unpacked", State)),
	?assertEqual(
		{true, State}, 
		do_acquire_lock(prepare, "storage_module_3_unpacked", State)),
	?assertEqual(
		{false, State}, 
		do_acquire_lock(prepare, "storage_module_4_unpacked", State)),
	
	?assertEqual(
		{true, State#state{store_id_locks = #{
			"storage_module_0_unpacked" => repack,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
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
		{true, State}, 
		do_acquire_lock(repack, "storage_module_5_unpacked", State)).

test_release_lock() ->
	State = #state{
		store_id_to_device = #{
			"storage_module_0_unpacked" => "device1",
			"storage_module_1_unpacked" => "device1",
			"storage_module_2_unpacked" => "device2",
			"storage_module_3_unpacked" => "device2",
			"storage_module_4_unpacked" => "device3",
			"storage_module_5_unpacked" => "device3",
			"storage_module_6_unpacked" => "device4"
		},
		device_locks = DeviceLocks = #{
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
		State#state{ device_locks = DeviceLocks#{ "device4" => sync }},
		do_release_lock(sync, "storage_module_6_unpacked", State)),

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

test_release_lock_without_device_limit() ->
	State = #state{
		store_id_to_device = #{
			"storage_module_0_unpacked" => "device1",
			"storage_module_1_unpacked" => "device1",
			"storage_module_2_unpacked" => "device2",
			"storage_module_3_unpacked" => "device2",
			"storage_module_4_unpacked" => "device3",
			"storage_module_5_unpacked" => "device3",
			"storage_module_6_unpacked" => "device4"
		},
		store_id_locks = StoreIDLocks = #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		},
		device_limit = false
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
		State#state{ store_id_locks = StoreIDLocks#{ "storage_module_6_unpacked" => sync }},
		do_release_lock(sync, "storage_module_6_unpacked", State)),

	?assertEqual(
		State, 
		do_release_lock(prepare, "storage_module_0_unpacked", State)),
	?assertEqual(
		State#state{store_id_locks =  #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => sync,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		}}, 
		do_release_lock(prepare, "storage_module_2_unpacked", State)),
	?assertEqual(
		State#state{store_id_locks = #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => sync,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		}},
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
		State#state{store_id_locks =  #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => sync,
			"storage_module_5_unpacked" => repack
		}}, 
		do_release_lock(repack, "storage_module_4_unpacked", State)),
	?assertEqual(
		State#state{store_id_locks =  #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => sync
		}}, 
		do_release_lock(repack, "storage_module_5_unpacked", State)).

test_count_prepare_locks() ->
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
			"device1" => {prepare, "storage_module_0_unpacked"},
			"device2" => {prepare, "storage_module_2_unpacked"},
			"device3" => {repack, "storage_module_4_unpacked"}
		},
		store_id_locks = #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => prepare,
			"storage_module_5_unpacked" => repack
		}
	},
	
	?assertEqual(2, count_prepare_locks(State#state{ device_limit = true })),
	?assertEqual(3, count_prepare_locks(State#state{ device_limit = false })).

test_log_locks() ->
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
		},
		store_id_locks = #{
			"storage_module_0_unpacked" => sync,
			"storage_module_1_unpacked" => sync,
			"storage_module_2_unpacked" => prepare,
			"storage_module_3_unpacked" => prepare,
			"storage_module_4_unpacked" => repack,
			"storage_module_5_unpacked" => repack
		}
	},

	?assertEqual(
		[
			[{event, device_lock_status}, {device, "device1"}, {store_id, "storage_module_0_unpacked"}, {status, sync}],
			[{event, device_lock_status}, {device, "device1"}, {store_id, "storage_module_1_unpacked"}, {status, sync}],
			[{event, device_lock_status}, {device, "device2"}, {store_id, "storage_module_2_unpacked"}, {status, prepare}],
			[{event, device_lock_status}, {device, "device2"}, {store_id, "storage_module_3_unpacked"}, {status, paused}],
			[{event, device_lock_status}, {device, "device3"}, {store_id, "storage_module_4_unpacked"}, {status, repack}],
			[{event, device_lock_status}, {device, "device3"}, {store_id, "storage_module_5_unpacked"}, {status, paused}]
		],
		do_log_locks(State#state{device_limit = true})),
	?assertEqual(
		[
			[{event, device_lock_status}, {device, "device1"}, {store_id, "storage_module_0_unpacked"}, {status, sync}],
			[{event, device_lock_status}, {device, "device1"}, {store_id, "storage_module_1_unpacked"}, {status, sync}],
			[{event, device_lock_status}, {device, "device2"}, {store_id, "storage_module_2_unpacked"}, {status, prepare}],
			[{event, device_lock_status}, {device, "device2"}, {store_id, "storage_module_3_unpacked"}, {status, prepare}],
			[{event, device_lock_status}, {device, "device3"}, {store_id, "storage_module_4_unpacked"}, {status, repack}],
			[{event, device_lock_status}, {device, "device3"}, {store_id, "storage_module_5_unpacked"}, {status, repack}]
		],
		do_log_locks(State#state{device_limit = false})).