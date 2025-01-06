-module(secp256k1_nif).
-export([sign/2]).

-on_load(init/0).

init() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "secp256k1_arweave"]), 0).

sign(_Digest, _PrivateBytes) ->
	erlang:nif_error(nif_not_loaded).
