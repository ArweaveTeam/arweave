-module(ar_mine_randomx).

-on_load(init_nif/0).

-export([
	init_fast/2, hash_fast/2,
	init_light/1, hash_light/2,
	release_state/1,
	bulk_hash_fast/4
]).

%% These exports are required for the DEBUG mode, where these functions are unused.
%% Also, some of these functions are used in ar_mine_randomx_tests.
-export([
	init_light_nif/3, hash_light_nif/5,
	init_fast_nif/4, hash_fast_nif/5, bulk_hash_fast_nif/9,
	release_state_nif/1, jit/0, large_pages/0, hardware_aes/0
]).

-include_lib("arweave/include/ar.hrl").

-ifdef(DEBUG).
init_fast(Key, _Threads) ->
	Key.
-else.
init_fast(Key, Threads) ->
	{ok, FastState} = init_fast_nif(Key, jit(), large_pages(), Threads),
	FastState.
-endif.

-ifdef(DEBUG).
hash_fast(FastState, Data) ->
	Hash = crypto:hash(sha256, << FastState/binary, Data/binary >>),
	list_to_binary(lists:sublist(binary_to_list(Hash), 48)).
-else.
hash_fast(FastState, Data) ->
	{ok, Hash} =
		hash_fast_nif(FastState, Data, jit(), large_pages(), hardware_aes()),
	Hash.
-endif.

-ifdef(DEBUG).
bulk_hash_fast(FastState, {Nonce, _}, BDS, _Diff) ->
	Hash = crypto:hash(sha256, << FastState/binary, Nonce/binary, BDS/binary >>),
	{list_to_binary(lists:sublist(binary_to_list(Hash), 48)), Nonce, Nonce, 1}.
-else.
bulk_hash_fast(FastState, {Nonce1, Nonce2}, BDS, Diff) ->
	{ok, Hash, HashNonce, ExtraNonce, HashesTried} =
		bulk_hash_fast_nif(FastState, Nonce1, Nonce2, BDS, binary:encode_unsigned(Diff, big), jit(), large_pages(), hardware_aes(), ar_meta_db:get(randomx_bulk_hashing_iterations)),
	{Hash, HashNonce, ExtraNonce, HashesTried}.
-endif.

-ifdef(DEBUG).
init_light(Key) ->
	Key.
-else.
init_light(Key) ->
	{ok, LightState} = init_light_nif(Key, jit(), large_pages()),
	LightState.
-endif.

-ifdef(DEBUG).
hash_light(LightState, Data) ->
	hash_fast(LightState, Data).
-else.
hash_light(LightState, Data) ->
	{ok, Hash} =
		hash_light_nif(LightState, Data, jit(), large_pages(), hardware_aes()),
	Hash.
-endif.

-ifdef(DEBUG).
release_state(_State) ->
	ok.
-else.
release_state(State) ->
	case release_state_nif(State) of
		ok ->
			?LOG_INFO([ar_mine_randomx, succeeded_releasing_randomx_state]),
			ok;
		{error, Reason} ->
			?LOG_WARNING([ar_mine_randomx, failed_to_release_randomx_state, {reason, Reason}]),
			error
	end.
-endif.

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

bulk_hash_fast_nif(
	_State, _Nonce1, _Nonce2, _BDS, _Diff, _JIT, _LargePages, _HardwareAES, _HashingIterations
) ->
	erlang:nif_error(nif_not_loaded).

hash_light_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

release_state_nif(_State) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "arweave"]), 0).
