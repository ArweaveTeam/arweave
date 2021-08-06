-module(ar_randomx_state).

-export([
	init/2, init/4,
	start/0, start_block_polling/0, reset/0,
	hash/2,
	randomx_state_by_height/1,
	swap_height/1,
	debug_server/0
]).

-include_lib("arweave/include/ar.hrl").

-record(state, {
	randomx_states,
	key_cache,
	next_key_gen_ahead
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

init(BI, Peers) ->
	CurrentHeight = length(BI) - 1,
	SwapHeights = lists:usort([
		swap_height(CurrentHeight + ?STORE_BLOCKS_BEHIND_CURRENT),
		swap_height(max(0, CurrentHeight - ?STORE_BLOCKS_BEHIND_CURRENT))
	]),
	Init = fun(SwapHeight) ->
		{ok, Key} = randomx_key(SwapHeight, BI, Peers),
		init(whereis(?MODULE), SwapHeight, Key, erlang:system_info(schedulers_online))
	end,
	lists:foreach(Init, SwapHeights).

init(Server, SwapHeight, Key, Threads) ->
	ar:console(
		"Initialising RandomX dataset for fast hashing. Swap height: ~p, Key: ~p. "
		"The process may take several minutes.~n", [SwapHeight, ar_util:encode(Key)]
	),
	Server ! {add_randomx_state, SwapHeight, {fast, ar_mine_randomx:init_fast(Key, Threads)}},
	ar:console("RandomX dataset initialisation for swap height ~p complete.~n", [SwapHeight]).

start() ->
	Pid = spawn_link(fun server/0),
	register(?MODULE, Pid),
	Pid.

start_block_polling() ->
	whereis(?MODULE) ! poll_new_blocks.

reset() ->
	whereis(?MODULE) ! reset.

hash(Height, Data) ->
	case randomx_state_by_height(Height) of
		{state, {fast, FastState}} ->
			ar_mine_randomx:hash_fast(FastState, Data);
		{state, {light, LightState}} ->
			ar_mine_randomx:hash_light(LightState, Data);
		{key, Key} ->
			LightState = ar_mine_randomx:init_light(Key),
			ar_mine_randomx:hash_light(LightState, Data)
	end.

randomx_state_by_height(Height) when is_integer(Height) andalso Height >= 0 ->
	Ref = make_ref(),
	whereis(?MODULE) ! {get_state_by_height, Height, Ref, self()},
	receive
		{Ref, {state_by_height, {ok, State}}} ->
			{state, State};
		{Ref, {state_by_height, {state_not_found, key_not_found}}} ->
			SwapHeight = swap_height(Height),
			{ok, Key} = randomx_key(SwapHeight),
			{key, Key};
		{Ref, {state_by_height, {state_not_found, Key}}} ->
			{key, Key}
	end.

swap_height(Height) ->
	(Height div ?RANDOMX_KEY_SWAP_FREQ) * ?RANDOMX_KEY_SWAP_FREQ.

debug_server() ->
	whereis(?MODULE) ! {get_state, self()},
	receive
		{state, State} -> State
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

server() ->
	server(init_state()).

init_state() ->
	set_next_key_gen_ahead(#state{
		randomx_states = #{},
		key_cache = #{}
	}).

set_next_key_gen_ahead(State) ->
	State#state{
		next_key_gen_ahead = rand_int(?RANDOMX_MIN_KEY_GEN_AHEAD, ?RANDOMX_MAX_KEY_GEN_AHEAD)
	}.

rand_int(Min, Max) ->
	rand:uniform(Max - Min) + Min - 1.

server(State) ->
	NewState = receive
		poll_new_blocks ->
			poll_new_blocks(State);
		{add_randomx_state, SwapHeight, RandomxState} ->
			State#state{
				randomx_states = maps:put(SwapHeight, RandomxState, State#state.randomx_states)
			};
		{get_state_by_height, Height, Ref, From} ->
			case maps:find(swap_height(Height), State#state.randomx_states) of
				error ->
					Key = get_key_from_cache(State, Height),
					From ! {Ref, {state_by_height, {state_not_found, Key}}};
				{ok, initializing} ->
					Key = get_key_from_cache(State, Height),
					From ! {Ref, {state_by_height, {state_not_found, Key}}};
				{ok, RandomxState} ->
					From ! {Ref, {state_by_height, {ok, RandomxState}}}
			end,
			State;
		{get_state, From} ->
			From ! {state, State},
			State;
		{cache_randomx_key, SwapHeight, Key} ->
			State#state{ key_cache = maps:put(SwapHeight, Key, State#state.key_cache) };
		reset ->
			init_state()
	end,
	server(NewState).

poll_new_blocks(State) ->
	case ar_node:is_joined() of
		false ->
			%% Add an extra poll soon
			timer:send_after(1000, poll_new_blocks),
			State;
		true ->
			Height = ar_node:get_height(),
			NewState = handle_new_height(State, Height),
			timer:send_after(?RANDOMX_STATE_POLL_INTERVAL * 1000, poll_new_blocks),
			NewState
	end.

handle_new_height(State, CurrentHeight) ->
	State1 = remove_old_randomx_states(State, CurrentHeight),
	case ensure_initialized(State1, swap_height(CurrentHeight)) of
		{started, State2} ->
			maybe_init_next(State2, CurrentHeight);
		did_not_start ->
			maybe_init_next(State1, CurrentHeight)
	end.

remove_old_randomx_states(State, CurrentHeight) ->
	Threshold = swap_height(CurrentHeight - ?RANDOMX_KEEP_KEY),
	IsOutdated = fun(SwapHeight) ->
		SwapHeight < Threshold
	end,
	RandomxStates = State#state.randomx_states,
	RemoveKeys = lists:filter(
		IsOutdated,
		maps:keys(RandomxStates)
	),
	%% RandomX allocates the memory for the dataset internally, bypassing enif_alloc.
	%% This presumably causes GC to be very reluctant to release the memory. Here we
	%% explicitly trigger the release. It is scheduled to happen after some time to account
	%% for other processes possibly still using it. In case some process is still using it,
	%% the release call will fail, leaving it for GC to handle.
	lists:foreach(
		fun(Key) ->
			{_, S} = maps:get(Key, RandomxStates),
			timer:apply_after(60000, ar_mine_randomx, release_state, [S])
		end,
		RemoveKeys
	),
	State#state{
		randomx_states = maps_remove_multi(RemoveKeys, RandomxStates)
	}.

maps_remove_multi([], Map) ->
	Map;
maps_remove_multi([Key | Keys], Map) ->
	maps_remove_multi(Keys, maps:remove(Key, Map)).

maybe_init_next(State, CurrentHeight) ->
	NextSwapHeight = swap_height(CurrentHeight) + ?RANDOMX_KEY_SWAP_FREQ,
	case NextSwapHeight - State#state.next_key_gen_ahead of
		_InitHeight when CurrentHeight >= _InitHeight ->
			case ensure_initialized(State, NextSwapHeight) of
				{started, NewState} -> set_next_key_gen_ahead(NewState);
				did_not_start -> State
			end;
		_ ->
			State
	end.

ensure_initialized(State, SwapHeight) ->
	case maps:find(SwapHeight, State#state.randomx_states) of
		{ok, _} ->
			did_not_start;
		error ->
			{started, start_init(State, SwapHeight)}
	end.

get_key_from_cache(State, Height) ->
	maps:get(swap_height(Height), State#state.key_cache, key_not_found).

start_init(State, SwapHeight) ->
	Server = self(),
	spawn_link(fun() ->
		init(Server, SwapHeight, 1)
	end),
	State#state{
		randomx_states = maps:put(SwapHeight, initializing, State#state.randomx_states)
	}.

init(Server, SwapHeight, Threads) ->
	case randomx_key(SwapHeight) of
		{ok, Key} ->
			init(Server, SwapHeight, Key, Threads);
		unavailable ->
			?LOG_WARNING([
				{event, ar_randomx_state_failed_to_read_or_download_key_block},
				{swap_height, SwapHeight}
			]),
			timer:sleep(5000),
			init(Server, SwapHeight, Threads)
	end.

%% @doc Return the key used in RandomX by key swap height. The key is the
%% dependent hash from the block at the previous swap height. If RandomX is used
%% already by the first ?RANDOMX_KEY_SWAP_FREQ blocks, then a hardcoded key is
%% used since there is no old enough block to fetch the key from.
%% @end
randomx_key(SwapHeight) when SwapHeight < ?RANDOMX_KEY_SWAP_FREQ ->
	{ok, <<"Arweave Genesis RandomX Key">>};
randomx_key(SwapHeight) ->
	KeyBlockHeight = SwapHeight - ?RANDOMX_KEY_SWAP_FREQ,
	case get_block(KeyBlockHeight) of
		{ok, KeyB} ->
			Key = KeyB#block.hash,
			whereis(?MODULE) ! {cache_randomx_key, SwapHeight, Key},
			{ok, Key};
		unavailable ->
			unavailable
	end.

get_block(Height) ->
	case ar_node:get_block_index() of
		[] -> unavailable;
		BI ->
			{BH, _, _} = lists:nth(Height + 1, lists:reverse(BI)),
			get_block(BH, BI)
	end.

get_block(BH, BI) ->
	try
		Peers = ar_bridge:get_remote_peers(),
		get_block(BH, BI, Peers)
	catch Type:Exception:StackTrace ->
		?LOG_ERROR([
			{event, randomx_state_server_failed_to_fetch_peers},
			{error, io_lib:format("~p ~p ~p", [Type, Exception, StackTrace])}
		]),
		get_block(BH, BI)
	end.

get_block(Height, BI, Peers) when is_integer(Height) ->
	{BH, _, _} = lists:nth(Height + 1, lists:reverse(BI)),
	get_block(BH, BI, Peers);
get_block(BH, BI, Peers) ->
	case ar_storage:read_block(BH) of
		B when is_record(B, block) ->
			{ok, B};
		unavailable ->
			case ar_http_iface_client:get_block_shadow(Peers, BH) of
				unavailable ->
					unavailable;
				{_, B} ->
					case ar_weave:indep_hash(B) of
						BH ->
							{ok, B};
						InvalidBH ->
							?LOG_WARNING([
								{event, ar_randomx_state_got_invalid_block},
								{requested_block_hash, ar_util:encode(BH)},
								{received_block_hash, ar_util:encode(InvalidBH)}
							]),
							get_block(BH, BI)
					end
			end
	end.

randomx_key(SwapHeight, _, _) when SwapHeight < ?RANDOMX_KEY_SWAP_FREQ ->
	randomx_key(SwapHeight);
randomx_key(SwapHeight, BI, Peers) ->
	KeyBlockHeight = SwapHeight - ?RANDOMX_KEY_SWAP_FREQ,
	case get_block(KeyBlockHeight, BI, Peers) of
		{ok, KeyB} ->
			{ok, KeyB#block.hash};
		unavailable ->
			unavailable
	end.
