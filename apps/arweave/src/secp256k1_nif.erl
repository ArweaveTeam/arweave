-module(secp256k1_nif).
-export([sign/2, ecrecover/2]).

-on_load(init/0).

-define(SigUpperBound, binary:decode_unsigned(<<16#7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0:256>>)).
-define(SigDiv, binary:decode_unsigned(<<16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141:256>>)).

init() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "secp256k1_arweave"]), 0).

sign_recoverable(_Digest, _PrivateBytes) ->
	erlang:nif_error(nif_not_loaded).

recover_pk_and_verify(_Digest, _Signature) ->
	erlang:nif_error(nif_not_loaded).

sign(Msg, PrivBytes) ->
	Digest = crypto:hash(sha256, Msg),
	{ok, Signature} = sign_recoverable(Digest, PrivBytes),
	Signature.

ecrecover(Msg, Signature) ->
	Digest = crypto:hash(sha256, Msg),
	case recover_pk_and_verify(Digest, Signature) of
		{ok, true, PubKey} -> {true, PubKey};
		{ok, false, _PubKey} -> {false, <<>>};
		{error, _Reason} -> {false, <<>>}
	end.
