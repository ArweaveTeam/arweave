-module(secp256k1_nif).
-export([generate_key/0]).

-on_load(init/0).

init() ->
    PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "secp256k1_arweave"]), 0).

generate_key() ->
    erlang:nif_error(not_loaded).
