-module(ar_mine_randomx).
-on_load(init_nif/0).
-export([init_fast/2, hash_fast/2, init_light/1, hash_light/2]).
-export([release_state/1]).
-export([bulk_hash_fast/4]).

init_fast(Key, Threads) ->
	{ok, FastState} = init_fast_nif(Key, jit(), large_pages(), Threads),
	FastState.

hash_fast(FastState, Data) ->
	{ok, Hash} =
		hash_fast_nif(FastState, Data, jit(), large_pages(), hardware_aes()),
	Hash.

bulk_hash_fast(FastState, Nonce, BDS, Diff) ->
	{ok, Hash, HashNonce, HashesTried} =
		bulk_hash_fast_nif(FastState, Nonce, BDS, binary:encode_unsigned(Diff, big), jit(), large_pages(), hardware_aes()),
	{Hash, HashNonce, HashesTried}.

init_light(Key) ->
	{ok, LightState} = init_light_nif(Key, jit(), large_pages()),
	LightState.

hash_light(LightState, Data) ->
	{ok, Hash} =
		hash_light_nif(LightState, Data, jit(), large_pages(), hardware_aes()),
	Hash.

release_state(State) ->
	case release_state_nif(State) of
		ok ->
			ar:info([ar_mine_randomx, succeeded_releasing_randomx_state]),
			ok;
		{error, Reason} ->
			ar:warn([ar_mine_randomx, failed_to_release_randomx_state, {reason, Reason}]),
			error
	end.

%% Internal

jit() ->
	case ar_meta_db:get(randomx_jit) of
		false ->
			0;
		_ ->
			1
	end.

large_pages() ->
	case ar_meta_db:get(randomx_large_pages) of
		true ->
			1;
		_ ->
			0
	end.

hardware_aes() ->
	case ar_meta_db:get(randomx_hardware_aes) of
		false ->
			0;
		_ ->
			1
	end.

init_fast_nif(_Key, _JIT, _LargePages, _Threads) ->
	erlang:nif_error(nif_not_loaded).

init_light_nif(_Key, _JIT, _LargePages) ->
	erlang:nif_error(nif_not_loaded).

hash_fast_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

bulk_hash_fast_nif(_State, _Nonce, _BDS, _Diff, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

hash_light_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

release_state_nif(_State) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "arweave"]), 0).
