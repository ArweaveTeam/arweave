-module(ar_mine_randomx).
-on_load(init_nif/0).
-export([init_fast/2, hash_fast/2, init_light/1, hash_light/2]).
-export([release_state/1]).

init_fast(Key, Threads) ->
	{ok, FastState} = init_fast_nif(Key, Threads),
	FastState.

hash_fast(FastState, Data) ->
	{ok, Hash} = hash_fast_nif(FastState, Data),
	Hash.

init_light(Key) ->
	{ok, LightState} = init_light_nif(Key),
	LightState.

hash_light(LightState, Data) ->
	{ok, Hash} = hash_light_nif(LightState, Data),
	Hash.

release_state(State) ->
	case release_state_nif(State) of
		ok ->
			ok;
		{error, Reason} ->
			ar:warn([ar_mine_randomx, failed_to_release_mining_state, {reason, Reason}]),
			error
	end.

%% Internal

init_fast_nif(_Key, _Threads) ->
	erlang:nif_error(nif_not_loaded).

init_light_nif(_Key) ->
	erlang:nif_error(nif_not_loaded).

hash_fast_nif(_State, _Data) ->
	erlang:nif_error(nif_not_loaded).

hash_light_nif(_State, _Data) ->
	erlang:nif_error(nif_not_loaded).

release_state_nif(_State) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	ok = erlang:load_nif("priv/arweave", 0).
