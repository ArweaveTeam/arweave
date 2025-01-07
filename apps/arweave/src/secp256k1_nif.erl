-module(secp256k1_nif).
-export([
    generate_key/0,
    sign/2, sign_recoverable/2,
    verify/3, verify_recoverable/3,
    recover_pk/2, recover_pk_and_verify/2
]).

-on_load(init/0).

init() ->
    PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "secp256k1_arweave"]), 0).

generate_key() ->
    erlang:nif_error(not_loaded).

sign(_Digest, _PrivateBytes) ->
    erlang:nif_error(nif_not_loaded).

verify(_Digest, _Signature, _PublicBytes) ->
    erlang:nif_error(nif_not_loaded).

sign_recoverable(_Digest, _PrivateBytes) ->
    erlang:nif_error(nif_not_loaded).

verify_recoverable(_Digest, _Signature, _PublicBytes) ->
    erlang:nif_error(nif_not_loaded).

recover_pk(_Digest, _Signature) ->
    erlang:nif_error(nif_not_loaded).

recover_pk_and_verify(_Digest, _Signature) ->
    erlang:nif_error(nif_not_loaded).
