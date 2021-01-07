%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014-2015, Andrew Bennett
%%% @doc
%%% Distributed under the Mozilla Public License v2.0.
%%% Original available at:
%%% https://github.com/potatosalad/erlang-crypto_rsassa_pss
%%% @end
%%% Created :  20 Jul 2015 by Andrew Bennett <andrew@pixid.com>
%%% Modified:  17 Nov 2017 by The Arweave Team <team@arweave.org>
%%%-------------------------------------------------------------------
-module(rsa_pss).

-include_lib("public_key/include/public_key.hrl").

%% API
-export([sign/3]).
-export([sign/4]).
-export([verify/4]).

%% Types
-type rsa_public_key()  :: #'RSAPublicKey'{}.
-type rsa_private_key() :: #'RSAPrivateKey'{}.
-type rsa_digest_type() :: 'md5' | 'sha' | 'sha224' | 'sha256' | 'sha384' | 'sha512'.

-define(PSS_TRAILER_FIELD, 16#BC).

-export([verify_legacy/4]).

%%====================================================================
%% API functions
%%====================================================================

-spec sign(Message, DigestType, PrivateKey) -> Signature
	when
		Message    :: binary() | {digest, binary()},
		DigestType :: rsa_digest_type() | atom(),
		PrivateKey :: rsa_private_key(),
		Signature  :: binary().
sign(Message, DigestType, PrivateKey) when is_binary(Message) ->
	sign({digest, crypto:hash(DigestType, Message)}, DigestType, PrivateKey);
sign(Message={digest, _}, DigestType, PrivateKey) ->
	SaltLen = byte_size(crypto:hash(DigestType, <<>>)),
	Salt = crypto:strong_rand_bytes(SaltLen),
	sign(Message, DigestType, Salt, PrivateKey).

-spec sign(Message, DigestType, Salt, PrivateKey) -> Signature
	when
		Message    :: binary() | {digest, binary()},
		DigestType :: rsa_digest_type() | atom(),
		Salt       :: binary(),
		PrivateKey :: rsa_private_key(),
		Signature  :: binary().
sign(Message, DigestType, Salt, PrivateKey) when is_binary(Message) ->
	sign({digest, crypto:hash(DigestType, Message)}, DigestType, Salt, PrivateKey);
sign({digest, Digest}, DigestType, Salt, PrivateKey=#'RSAPrivateKey'{modulus=N}) ->
	DigestLen = byte_size(Digest),
	SaltLen = byte_size(Salt),
	PublicBitSize = int_to_bit_size(N),
	PrivateByteSize = (PublicBitSize + 7) div 8,
	PublicByteSize = int_to_byte_size(N),
	case PublicByteSize < (DigestLen + SaltLen + 2) of
		false ->
			DBLen = PrivateByteSize - DigestLen - 1,
			M = << 0:64, Digest/binary, Salt/binary >>,
			H = crypto:hash(DigestType, M),
			DB = << 0:((DBLen - SaltLen - 1) * 8), 1, Salt/binary >>,
			DBMask = mgf1(DigestType, H, DBLen),
			MaskedDB = normalize_to_key_size(PublicBitSize, crypto:exor(DB, DBMask)),
			EM = << MaskedDB/binary, H/binary, ?PSS_TRAILER_FIELD >>,
			DM = pad_to_key_size(PublicByteSize, dp(EM, PrivateKey)),
			DM;
		true ->
			erlang:error(badarg, [{digest, Digest}, DigestType, Salt, PrivateKey])
	end.

-spec verify(Message, DigestType, Signature, PublicKey) -> boolean()
	when
		Message    :: binary() | {digest, binary()},
		DigestType :: rsa_digest_type() | atom(),
		Signature  :: binary(),
		PublicKey  :: rsa_public_key().
verify(Message, DigestType, Signature, PublicKey) when is_binary(Message) ->
	verify({digest, crypto:hash(DigestType, Message)}, DigestType, Signature, PublicKey);
verify({digest, Digest}, DigestType, Signature, PublicKey=#'RSAPublicKey'{modulus=N}) ->
	DigestLen = byte_size(Digest),
	PublicBitSize = int_to_bit_size(N),
	PrivateByteSize = (PublicBitSize + 7) div 8,
	PublicByteSize = int_to_byte_size(N),
	SignatureSize = byte_size(Signature),
	case PublicByteSize =:= SignatureSize of
		true ->
			SignatureNumber = binary:decode_unsigned(Signature, big),
			case SignatureNumber >= 0 andalso SignatureNumber < N of
				true ->
					DBLen = PrivateByteSize - DigestLen - 1,
					EM = pad_to_key_size(PrivateByteSize, ep(Signature, PublicKey)),
					case binary:last(EM) of
						?PSS_TRAILER_FIELD ->
							MaskedDB = binary:part(EM, 0, byte_size(EM) - DigestLen - 1),
							H = binary:part(EM, byte_size(MaskedDB), DigestLen),
							DBMask = mgf1(DigestType, H, DBLen),
							DB = normalize_to_key_size(PublicBitSize, crypto:exor(MaskedDB, DBMask)),
							case binary:match(DB, << 1 >>) of
								{Pos, Len} ->
									PS = binary:decode_unsigned(binary:part(DB, 0, Pos)),
									case PS =:= 0 of
										true ->
											Salt = binary:part(DB, Pos + Len, byte_size(DB) - Pos - Len),
											M = << 0:64, Digest/binary, Salt/binary >>,
											HOther = crypto:hash(DigestType, M),
											H =:= HOther;
										false ->
											false
									end;
								nomatch ->
									false
							end;
						_BadTrailer ->
							false
					end;
				_ ->
					false
			end;
		false ->
			false
	end.

verify_legacy(Message, DigestType, Signature, PublicKey) when is_binary(Message) ->
	verify_legacy({digest, crypto:hash(DigestType, Message)}, DigestType, Signature, PublicKey);
verify_legacy({digest, Digest}, DigestType, Signature, PublicKey=#'RSAPublicKey'{modulus=N}) ->
	DigestLen = byte_size(Digest),
	PublicBitSize = int_to_bit_size(N),
	PrivateByteSize = PublicBitSize div 8,
	PublicByteSize = int_to_byte_size(N),
	SignatureSize = byte_size(Signature),
	case PublicByteSize =:= SignatureSize of
		true ->
			SignatureNumber = binary:decode_unsigned(Signature, big),
			case SignatureNumber >= 0 andalso SignatureNumber < N of
				true ->
					DBLen = PrivateByteSize - DigestLen - 1,
					EM = pad_to_key_size(PrivateByteSize, ep(Signature, PublicKey)),
					case binary:last(EM) of
						?PSS_TRAILER_FIELD ->
							MaskedDB = binary:part(EM, 0, byte_size(EM) - DigestLen - 1),
							H = binary:part(EM, byte_size(MaskedDB), DigestLen),
							DBMask = mgf1(DigestType, H, DBLen),
							DB = normalize_to_key_size(PublicBitSize, crypto:exor(MaskedDB, DBMask)),
							case binary:match(DB, << 1 >>) of
								{Pos, Len} ->
									PS = binary:decode_unsigned(binary:part(DB, 0, Pos)),
									case PS =:= 0 of
										true ->
											Salt = binary:part(DB, Pos + Len, byte_size(DB) - Pos - Len),
											M = << 0:64, Digest/binary, Salt/binary >>,
											HOther = crypto:hash(DigestType, M),
											H =:= HOther;
										false ->
											false
									end;
								nomatch ->
									false
							end;
						_BadTrailer ->
							false
					end;
				_ ->
					false
			end;
		false ->
			false
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
dp(B, #'RSAPrivateKey'{modulus=N, privateExponent=E}) ->
	crypto:mod_pow(B, E, N).

%% @private
ep(B, #'RSAPublicKey'{modulus=N, publicExponent=E}) ->
	crypto:mod_pow(B, E, N).

%% @private
int_to_bit_size(I) ->
	int_to_bit_size(I, 0).

%% @private
int_to_bit_size(0, B) ->
	B;
int_to_bit_size(I, B) ->
	int_to_bit_size(I bsr 1, B + 1).

%% @private
int_to_byte_size(I) ->
	int_to_byte_size(I, 0).

%% @private
int_to_byte_size(0, B) ->
	B;
int_to_byte_size(I, B) ->
	int_to_byte_size(I bsr 8, B + 1).

%% @private
mgf1(DigestType, Seed, Len) ->
	mgf1(DigestType, Seed, Len, <<>>, 0).

%% @private
mgf1(_DigestType, _Seed, Len, T, _Counter) when byte_size(T) >= Len ->
	binary:part(T, 0, Len);
mgf1(DigestType, Seed, Len, T, Counter) ->
	CounterBin = << Counter:8/unsigned-big-integer-unit:4 >>,
	NewT = << T/binary, (crypto:hash(DigestType, << Seed/binary, CounterBin/binary >>))/binary >>,
	mgf1(DigestType, Seed, Len, NewT, Counter + 1).

%% @private
normalize_to_key_size(_, <<>>) ->
	<<>>;
normalize_to_key_size(Bits, _A = << C, Rest/binary >>) ->
	SH = (Bits - 1) band 16#7,
	Mask = case SH > 0 of
		false ->
			16#FF;
		true ->
			16#FF bsr (8 - SH)
	end,
	B = << (C band Mask), Rest/binary >>,
	B.

%% @private
pad_to_key_size(Bytes, Data) when byte_size(Data) < Bytes ->
	pad_to_key_size(Bytes, << 0, Data/binary >>);
pad_to_key_size(_Bytes, Data) ->
	Data.
