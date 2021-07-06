%%% @doc The module with utilities for transaction creation, signing and verification.
-module(ar_tx).

-export([
	new/0, new/1, new/2, new/3, new/4,
	sign/2, sign/3,
	sign_v1/2, sign_v1/3,
	verify/5, verify/6,
	verify_tx_id/2,
	tags_to_list/1,
	get_tx_fee/4, get_tx_fee/6,
	check_last_tx/2,
	generate_chunk_tree/1, generate_chunk_tree/2, generate_chunk_id/1,
	chunk_binary/2, chunks_to_size_tagged_chunks/1, sized_chunks_to_sized_chunk_ids/1,
	get_addresses/1
]).

-export([get_wallet_fee_pre_fork_2_4/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc A helper for preparing transactions for signing. Used in tests.
%% Should be moved to a testing module.
%% @end
new() ->
	#tx{ id = crypto:strong_rand_bytes(32) }.
new(Data) ->
	#tx{ id = crypto:strong_rand_bytes(32), data = Data, data_size = byte_size(Data) }.
new(Data, Reward) ->
	#tx{
		id = crypto:strong_rand_bytes(32),
		data = Data,
		reward = Reward,
		data_size = byte_size(Data)
	}.
new(Data, Reward, Last) ->
	#tx{
		id = crypto:strong_rand_bytes(32),
		last_tx = Last,
		data = Data,
		data_size = byte_size(Data),
		reward = Reward
	}.
new({_, Dest}, Reward, Qty, Last) ->
	%% When Dest is the destination public key it can have a bit size of ?HASH_SZ
	%% which if uncaught will leave #tx.target unhashed. We accept a tuple of {KeyType, Dest}
	%% here so the caller can specify that they're not passing an address.
	new(ar_wallet:to_address(Dest), Reward, Qty, Last);
new(Dest, Reward, Qty, Last) when bit_size(Dest) == ?HASH_SZ ->
	#tx{
		id = crypto:strong_rand_bytes(32),
		last_tx = Last,
		quantity = Qty,
		target = Dest,
		data = <<>>,
		data_size = 0,
		reward = Reward
	};
new(Dest, Reward, Qty, Last) ->
	%% Convert wallets to addresses before building transactions.
	new(ar_wallet:to_address(Dest), Reward, Qty, Last).

%% @doc Cryptographically sign (claim ownership of) a v2 transaction.
%% Used in tests and by the handler of the POST /unsigned_tx endpoint, which is
%% disabled by default.
%% @end
sign(TX, {PrivKey, PubKey = {_, Owner}}) ->
	sign(TX, PrivKey, PubKey, signature_data_segment_v2(TX#tx{ owner = Owner })).

sign(TX, PrivKey, PubKey = {_, Owner}) ->
	sign(TX, PrivKey, PubKey, signature_data_segment_v2(TX#tx{ owner = Owner })).

%% @doc Cryptographically sign (claim ownership of) a v1 transaction.
%% Used in tests and by the handler of the POST /unsigned_tx endpoint, which is
%% disabled by default.
%% @end
sign_v1(TX, {PrivKey, PubKey = {_, Owner}}) ->
	sign(TX, PrivKey, PubKey, signature_data_segment_v1(TX#tx{ owner = Owner })).

sign_v1(TX, PrivKey, PubKey = {_, Owner}) ->
	sign(TX, PrivKey, PubKey, signature_data_segment_v1(TX#tx{ owner = Owner })).

%% @doc Verify whether a transaction is valid.
%% Signature verification can be optionally skipped, useful for
%% repeatedly checking mempool transactions' validity.
%% @end
verify(TX, Rate, Height, Wallets, Timestamp) ->
	verify(TX, Rate, Height, Wallets, Timestamp, verify_signature).

-ifdef(DEBUG).
verify(#tx { signature = <<>> }, _, _, _, _, _) -> true;
verify(TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	do_verify(TX, Rate, Height, Wallets, Timestamp, VerifySignature).
-else.
verify(TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	do_verify(TX, Rate, Height, Wallets, Timestamp, VerifySignature).
-endif.

%% @doc Verify the given transaction actually has the given identifier.
%% Compute the signature data segment, verify the signature, and check
%% whether its SHA2-256 hash equals the expected identifier.
%% @end
verify_tx_id(ExpectedID, #tx{ format = 1, id = ID } = TX) ->
	ExpectedID == ID andalso verify_signature_v1(TX, verify_signature) andalso verify_hash(TX);
verify_tx_id(ExpectedID, #tx{ format = 2, id = ID } = TX) ->
	ExpectedID == ID andalso verify_signature_v2(TX, verify_signature) andalso verify_hash(TX).

tags_to_list(Tags) ->
	[[Name, Value] || {Name, Value} <- Tags].

-ifdef(DEBUG).
check_last_tx(_WalletList, TX) when TX#tx.owner == <<>> -> true;
check_last_tx(WalletList, _TX) when map_size(WalletList) == 0 ->
	true;
check_last_tx(WalletList, TX) ->
	Addr = ar_wallet:to_address(TX#tx.owner),
	case maps:get(Addr, WalletList, not_found) of
		not_found ->
			false;
		{_Balance, LastTX} ->
			LastTX == TX#tx.last_tx
	end.
-else.
%% @doc Check if the given transaction anchors one of the wallets - its last_tx
%% matches the last transaction made from the wallet.
%% @end
check_last_tx(WalletList, _TX) when map_size(WalletList) == 0 ->
	true;
check_last_tx(WalletList, TX) ->
	Addr = ar_wallet:to_address(TX#tx.owner),
	case maps:get(Addr, WalletList, not_found) of
		not_found ->
			false;
		{_Balance, LastTX} ->
			LastTX == TX#tx.last_tx
	end.
-endif.

%% @doc Split the tx data into chunks and compute the Merkle tree from them.
%% Used to compute the Merkle roots of v1 transactions' data and to compute
%% Merkle proofs for v2 transactions when their data is uploaded without proofs.
%% @end
generate_chunk_tree(TX) ->
	generate_chunk_tree(TX,
		sized_chunks_to_sized_chunk_ids(
			chunks_to_size_tagged_chunks(
				chunk_binary(?DATA_CHUNK_SIZE, TX#tx.data)
			)
		)
	).

generate_chunk_tree(TX, ChunkIDSizes) ->
	{Root, Tree} = ar_merkle:generate_tree(ChunkIDSizes),
	TX#tx { data_tree = Tree, data_root = Root }.

%% @doc Generate a chunk ID used to construct the Merkle tree from the tx data chunks.
generate_chunk_id(Chunk) ->
	crypto:hash(sha256, Chunk).

%% @doc Split the binary into chunks. Used for computing the Merkle roots of
%% v1 transactions' data and computing Merkle proofs for v2 transactions' when
%% their data is uploaded without proofs.
%% @end
chunk_binary(ChunkSize, Bin) when byte_size(Bin) < ChunkSize ->
	[Bin];
chunk_binary(ChunkSize, Bin) ->
	<<ChunkBin:ChunkSize/binary, Rest/binary>> = Bin,
	[ChunkBin | chunk_binary(ChunkSize, Rest)].

%% @doc Assign a byte offset to every chunk in the list.
chunks_to_size_tagged_chunks(Chunks) ->
	lists:reverse(
		element(
			2,
			lists:foldl(
				fun(Chunk, {Pos, List}) ->
					End = Pos + byte_size(Chunk),
					{End, [{Chunk, End} | List]}
				end,
				{0, []},
				Chunks
			)
		)
	).

%% @doc Convert a list of chunk, byte offset tuples to the list of chunk ID, byte offset tuples.
sized_chunks_to_sized_chunk_ids(SizedChunks) ->
	[{ar_tx:generate_chunk_id(Chunk), Size} || {Chunk, Size} <- SizedChunks].

%% @doc Get a list of unique source and destination addresses from the given list of txs.
get_addresses(TXs) ->
	get_addresses(TXs, sets:new()).

get_wallet_fee_pre_fork_2_4(Diff, Height) ->
	case Height >= ar_fork:height_2_2() of
		true ->
			%% Scale the wallet fee so that is is always roughly 0.1$.
			{Dividend, Divisor} = ?WALLET_GEN_FEE_USD,
			ar_pricing:usd_to_ar_pre_fork_2_4(
				Dividend / Divisor,
				Diff,
				Height
			);
		false ->
			?WALLET_GEN_FEE
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Generate the data segment to be signed for a given v2 TX.
signature_data_segment_v2(TX = #tx { signature_type = TXSigType }) ->
	SigTypeTrailer =
		case TXSigType of
			undefined -> [];
			_ -> [list_to_binary(ar_serialize:signature_type_to_list(TXSigType))]
		end,
	ar_deep_hash:hash([
		<<(integer_to_binary(TX#tx.format))/binary>>,
		<<(TX#tx.owner)/binary>>,
		<<(TX#tx.target)/binary>>,
		<<(list_to_binary(integer_to_list(TX#tx.quantity)))/binary>>,
		<<(list_to_binary(integer_to_list(TX#tx.reward)))/binary>>,
		<<(TX#tx.last_tx)/binary>>,
		tags_to_list(TX#tx.tags),
		<<(integer_to_binary(TX#tx.data_size))/binary>>,
		<<(TX#tx.data_root)/binary>>
	] ++ SigTypeTrailer).

%% @doc Generate the data segment to be signed for a given v1 TX.
signature_data_segment_v1(T) ->
	<<
		(T#tx.owner)/binary,
		(T#tx.target)/binary,
		(T#tx.data)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary,
		(list_to_binary(integer_to_list(T#tx.reward)))/binary,
		(T#tx.last_tx)/binary,
		(tags_to_binary(T#tx.tags))/binary
	>>.

sign(TX, PrivKey, {_, Owner}, SignatureDataSegment) ->
	NewTX = TX#tx{ owner = Owner },
	Sig = ar_wallet:sign(PrivKey, SignatureDataSegment),
	ID = crypto:hash(?HASH_ALG, <<Sig/binary>>),
	NewTX#tx {
		id = ID,
		signature = Sig
		% signature_type = KeyType
	}.

do_verify(#tx{ format = 1 } = TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	do_verify_v1(TX, Rate, Height, Wallets, Timestamp, VerifySignature);
do_verify(#tx{ format = 2 } = TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	case Height < ar_fork:height_2_0() of
		true ->
			collect_validation_results(TX#tx.id, [{"tx_format_not_supported", false}]);
		false ->
			do_verify_v2(TX, Rate, Height, Wallets, Timestamp, VerifySignature)
	end;
do_verify(TX, _Rate, _Height, _Wallets, _Timestamp, _VerifySignature) ->
	collect_validation_results(TX#tx.id, [{"tx_format_not_supported", false}]).

get_addresses([], Addresses) ->
	sets:to_list(Addresses);
get_addresses([TX | TXs], Addresses) ->
	Source = ar_wallet:to_address(TX#tx.owner),
	WithSource = sets:add_element(Source, Addresses),
	WithDest = sets:add_element(TX#tx.target, WithSource),
	get_addresses(TXs, WithDest).

do_verify_v1(TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	Fork_1_8 = ar_fork:height_1_8(),
	LastTXCheck = case Height of
		H when H >= Fork_1_8 ->
			true;
		_ ->
			check_last_tx(Wallets, TX)
	end,
	Checks = [
		{"quantity_negative",
		 TX#tx.quantity >= 0},
		{"same_owner_as_target",
		 (ar_wallet:to_address(TX#tx.owner) =/= TX#tx.target)},
		{"tx_too_cheap",
		 is_tx_fee_sufficient(TX, Rate, Height, Wallets, TX#tx.target, Timestamp)},
		{"tx_fields_too_large",
		 tx_field_size_limit_v1(TX, Height)},
		{"tag_field_illegally_specified",
		 tag_field_legal(TX)},
		{"last_tx_not_valid",
		 LastTXCheck},
		{"tx_id_not_valid",
		 verify_hash(TX)},
		{"overspend",
		 validate_overspend(TX, ar_node_utils:apply_tx(Wallets, TX, Height))},
		{"tx_signature_not_valid",
		 verify_signature_v1(TX, VerifySignature, Height)},
		{"tx_malleable",
		 verify_malleability(TX, Rate, Height, Wallets, Timestamp)},
		{"no_target",
		 verify_target_length(TX, Height)}
	],
	collect_validation_results(TX#tx.id, Checks).

collect_validation_results(TXID, Checks) ->
	KeepFailed = fun
		({_, true}) ->
			false;
		({ErrorCode, false}) ->
			{true, ErrorCode}
	end,
	case lists:filtermap(KeepFailed, Checks) of
		[] ->
			true;
		ErrorCodes ->
			ar_tx_db:put_error_codes(TXID, ErrorCodes),
			false
	end.

do_verify_v2(TX, Rate, Height, Wallets, Timestamp, VerifySignature) ->
	Checks = [
		{"quantity_negative",
		 TX#tx.quantity >= 0},
		{"same_owner_as_target",
		 (ar_wallet:to_address(TX#tx.owner) =/= TX#tx.target)},
		{"tx_too_cheap",
		 is_tx_fee_sufficient(TX, Rate, Height, Wallets, TX#tx.target, Timestamp)},
		{"tx_fields_too_large",
		 tx_field_size_limit_v2(TX)},
		{"tag_field_illegally_specified",
		 tag_field_legal(TX)},
		{"tx_id_not_valid",
		 verify_hash(TX)},
		{"overspend",
		 validate_overspend(TX, ar_node_utils:apply_tx(Wallets, TX, Height))},
		{"tx_signature_type_not_valid",
		 verify_signature_type(TX, Height)},
		{"tx_signature_not_valid",
		 verify_signature_v2(TX, VerifySignature, Height)},
		{"tx_data_size_negative",
		 TX#tx.data_size >= 0},
		{"tx_data_size_data_root_mismatch",
		 (TX#tx.data_size == 0) == (TX#tx.data_root == <<>>)},
		{"no_target",
		 verify_target_length(TX, Height)}
	],
	collect_validation_results(TX#tx.id, Checks).

%% @doc Check whether each field in a transaction is within the given byte size limits.
tx_field_size_limit_v1(TX, Height) ->
	LastTXLimit =
		case Height >= ar_fork:height_1_8() of
			true ->
				48;
			false ->
				32
		end,
	case tag_field_legal(TX) of
		true ->
			(byte_size(TX#tx.id) =< 32) and
			(byte_size(TX#tx.last_tx) =< LastTXLimit) and
			(byte_size(TX#tx.owner) =< 512) and
			(byte_size(tags_to_binary(TX#tx.tags)) =< 2048) and
			(byte_size(integer_to_binary(TX#tx.quantity)) =< 21) and
			(byte_size(TX#tx.data) =< (?TX_DATA_SIZE_LIMIT)) and
			(byte_size(TX#tx.signature) =< 512) and
			(byte_size(integer_to_binary(TX#tx.reward)) =< 21);
		false -> false
	end.

%% @doc Check that the structure of the txs tag field is in the expected key value format.
tag_field_legal(TX) ->
	lists:all(
		fun(X) ->
			case X of
				{_, _} -> true;
				_ -> false
			end
		end,
		TX#tx.tags
	).

%% @doc Verify that the transactions ID is a hash of its signature.
verify_hash(#tx {signature = Sig, id = ID}) ->
	ID == crypto:hash(?HASH_ALG, <<Sig/binary>>).

verify_signature_v1(_TX, do_not_verify_signature) ->
	true;
verify_signature_v1(TX, verify_signature) ->
	SignatureDataSegment = signature_data_segment_v1(TX),
	ar_wallet:verify({?DEFAULT_KEY_TYPE, TX#tx.owner}, SignatureDataSegment, TX#tx.signature).

verify_signature_v1(_TX, do_not_verify_signature, _Height) ->
	true;
verify_signature_v1(TX, verify_signature, Height) ->
	SignatureDataSegment = signature_data_segment_v1(TX),
	case Height >= ar_fork:height_2_4() of
		true ->
			ar_wallet:verify({?DEFAULT_KEY_TYPE, TX#tx.owner}, SignatureDataSegment, TX#tx.signature);
		false ->
			ar_wallet:verify_pre_fork_2_4({?DEFAULT_KEY_TYPE, TX#tx.owner}, SignatureDataSegment, TX#tx.signature)
	end.

verify_malleability(TX, Rate, Height, Wallets, Timestamp) ->
	case Height + 1 >= ar_fork:height_2_4() of
		false ->
			true;
		true ->
			Target = TX#tx.target,
			case {byte_size(Target), TX#tx.quantity > 0} of
				{TargetSize, true} when TargetSize /= 32 ->
					false;
				{TargetSize, false} when TargetSize > 0 ->
					false;
				_ ->
					case ends_with_digit(TX#tx.data) of
						true ->
							false;
						false ->
							Fee = TX#tx.reward,
							case Fee < 10 of
								true ->
									true;
								false ->
									not is_tx_fee_sufficient(
										TX#tx{
											reward =
												list_to_integer(
													tl(integer_to_list(TX#tx.reward))
												)
										},
										Rate,
										Height,
										Wallets,
										Target,
										Timestamp
									)
							end
					end
			end
	end.

ends_with_digit(<<>>) ->
	false;
ends_with_digit(Data) ->
	LastByte = binary:last(Data),
	LastByte >= 48 andalso LastByte =< 57.

verify_signature_type(#tx { signature_type = TXSigType }, Height) ->
	case Height >= ar_fork:height_2_5() of
		true -> true;
		false -> TXSigType == undefined
	end.

verify_signature_v2(_TX, do_not_verify_signature) ->
	true;
verify_signature_v2(TX = #tx { signature_type = TXSigType }, verify_signature) ->
	SigType =
		case TXSigType of
			undefined -> ?DEFAULT_KEY_TYPE;
			_ -> TXSigType
		end,
	SignatureDataSegment = signature_data_segment_v2(TX),
	ar_wallet:verify({SigType, TX#tx.owner}, SignatureDataSegment, TX#tx.signature).

verify_signature_v2(_TX, do_not_verify_signature, _Height) ->
	true;
verify_signature_v2(TX = #tx { signature_type = TXSigType }, verify_signature, Height) ->
	SignatureDataSegment = signature_data_segment_v2(TX),
	case Height >= ar_fork:height_2_4() of
		true ->
			SigType =
				case Height >= ar_fork:height_2_5() of
					true ->
						case TXSigType of
							undefined -> ?DEFAULT_KEY_TYPE;
							_ -> TXSigType
						end;
					false ->
						?DEFAULT_KEY_TYPE
				end,
			ar_wallet:verify({SigType, TX#tx.owner}, SignatureDataSegment, TX#tx.signature);
		false ->
			ar_wallet:verify_pre_fork_2_4({?DEFAULT_KEY_TYPE, TX#tx.owner}, SignatureDataSegment, TX#tx.signature)
	end.

validate_overspend(TX, Wallets) ->
	From = ar_wallet:to_address(TX#tx.owner),
	Addresses = case TX#tx.target of
		<<>> ->
			[From];
		To ->
			[From, To]
	end,
	lists:all(
		fun(Addr) ->
			case maps:get(Addr, Wallets, not_found) of
				{0, Last} when byte_size(Last) == 0 ->
					false;
				{Quantity, _} when Quantity < 0 ->
					false;
				not_found ->
					false;
				_ ->
					true
			end
		end,
		Addresses
	).

%% @doc Ensure that transaction fee is sufficiently big.
is_tx_fee_sufficient(TX, Rate, Height, Wallets, Addr, Timestamp) ->
	TX#tx.reward >= get_tx_fee(TX#tx.data_size, Rate, Height + 1, Wallets, Addr, Timestamp).

%% @doc Calculate the minimum required transaction fee, including a wallet fee,
%% if `Addr` is not in `Wallets`.
%% @end
get_tx_fee(DataSize, Rate, Height, Wallets, Addr, Timestamp) ->
	true = Height >= ar_fork:height_2_4(),
	IncludesWalletFee = Addr /= <<>> andalso maps:get(Addr, Wallets, not_found) == not_found,
	case IncludesWalletFee of
		true ->
			WalletFee = ar_pricing:usd_to_ar(?WALLET_GEN_FEE_USD, Rate, Height),
			WalletFee + ar_pricing:get_tx_fee(DataSize, Timestamp, Rate, Height);
		false ->
			ar_pricing:get_tx_fee(DataSize, Timestamp, Rate, Height)
	end.

%% @doc Calculate the minimum required transaction fee, assuming no wallet fee.
get_tx_fee(DataSize, Rate, Height, Timestamp) ->
	true = Height >= ar_fork:height_2_4(),
	ar_pricing:get_tx_fee(DataSize, Timestamp, Rate, Height).

verify_target_length(TX, Height) ->
	case Height >= ar_fork:height_2_4() of
		true ->
			(TX#tx.quantity == 0 andalso byte_size(TX#tx.target) =< 32)
				orelse byte_size(TX#tx.target) == 32;
		false ->
			byte_size(TX#tx.target) =< 32
	end.

tx_field_size_limit_v2(TX) ->
	case tag_field_legal(TX) of
		true ->
			(byte_size(TX#tx.id) =< 32) and
			(byte_size(TX#tx.last_tx) =< 48) and
			(byte_size(TX#tx.owner) =< 512) and
			(byte_size(tags_to_binary(TX#tx.tags)) =< 2048) and
			(byte_size(integer_to_binary(TX#tx.quantity)) =< 21) and
			(byte_size(integer_to_binary(TX#tx.data_size)) =< 21) and
			(byte_size(TX#tx.signature) =< 512) and
			(byte_size(integer_to_binary(TX#tx.reward)) =< 21) and
			(byte_size(TX#tx.data_root) =< 32);
		false -> false
	end.

%% @doc Convert a transactions key-value tags to binary a format.
tags_to_binary(Tags) ->
	list_to_binary(
		lists:foldr(
			fun({Name, Value}, Acc) ->
				[Name, Value | Acc]
			end,
			[],
			Tags
		)
	).

%%%===================================================================
%%% Tests.
%%%===================================================================

sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(1)),
	{Priv, Pub} = ar_wallet:new(),
	Rate = ?USD_TO_AR_INITIAL_RATE,
	Timestamp = os:system_time(seconds),
	ValidTXs = [
		sign_v1(NewTX, Priv, Pub),
		sign(generate_chunk_tree(NewTX#tx{ format = 2 }), Priv, Pub)
	],
	lists:foreach(
		fun(TX) ->
			Wallets =
				lists:foldl(
					fun(Addr, Acc) ->
						maps:put(Addr, {?AR(10), <<>>}, Acc)
					end,
					#{},
					ar_tx:get_addresses([TX])
				),
			?assert(verify(TX, Rate, 0, Wallets, Timestamp), ar_util:encode(TX#tx.id)),
			?assert(verify(TX, Rate, 1, Wallets, Timestamp), ar_util:encode(TX#tx.id))
		end,
		ValidTXs
	),
	InvalidTXs = [
		sign(
			generate_chunk_tree( % a quantity with empty target
				NewTX#tx{ format = 2, quantity = 1 }
			),
			Priv,
			Pub
		),
		sign_v1(
			generate_chunk_tree( % a target without quantity
				NewTX#tx{ format = 1, target = crypto:strong_rand_bytes(32) }
			),
			Priv,
			Pub
		)
	],
	lists:foreach(
		fun(TX) ->
			Wallets =
				lists:foldl(
					fun(Addr, Acc) ->
						maps:put(Addr, {?AR(10), <<>>}, Acc)
					end,
					#{},
					ar_tx:get_addresses([TX])
				),
			?assert(not verify(TX, Rate, 0, Wallets, Timestamp), ar_util:encode(TX#tx.id)),
			?assert(not verify(TX, Rate, 1, Wallets, Timestamp), ar_util:encode(TX#tx.id))
		end,
		InvalidTXs
	).

sign_and_verify_chunked_test_() ->
	{timeout, 60, fun test_sign_and_verify_chunked/0}.

sign_and_verify_chunked_pre_fork_2_5_test_() ->
	ar_test_fork:test_on_fork(height_2_5, infinity, fun test_sign_and_verify_chunked/0).

test_sign_and_verify_chunked() ->
	TXData = crypto:strong_rand_bytes(trunc(?DATA_CHUNK_SIZE * 5.5)),
	{Priv, Pub} = ar_wallet:new(),
	UnsignedTX =
		generate_chunk_tree(
			#tx {
				format = 2,
				data = TXData,
				data_size = byte_size(TXData),
				reward = ?AR(100)
			}
		),
	SignedTX = sign(UnsignedTX#tx{ data = <<>> }, Priv, Pub),
	Height = 0,
	Rate = {1, 3},
	Timestamp = os:system_time(seconds),
	Address = ar_wallet:to_address(Pub),
	?assert(
		verify(
			SignedTX,
			Rate,
			Height,
			maps:from_list([{Address, {?AR(100), <<>>}}]),
			Timestamp
		)
	).

%% Ensure that a forged transaction does not pass verification.
forge_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	Rate = ?USD_TO_AR_INITIAL_RATE,
	Height = 0,
	InvalidSignTX = (sign_v1(NewTX, Priv, Pub))#tx {
		data = <<"FAKE DATA">>
	},
	Timestamp = os:system_time(seconds),
	?assert(not verify(InvalidSignTX, Rate, Height, #{}, Timestamp)).

%% Ensure that transactions above the minimum tx cost are accepted.
is_tx_fee_sufficient_test() ->
	ValidTX = new(<<"TEST DATA">>, ?AR(10)),
	InvalidTX = new(<<"TEST DATA">>, 1),
	Rate = {1, 5},
	Height = 123,
	Timestamp = os:system_time(seconds),
	?assert(is_tx_fee_sufficient(ValidTX, Rate, Height, #{}, <<"non-existing-addr">>, Timestamp)),
	?assert(
		not is_tx_fee_sufficient(InvalidTX, Rate, Height, #{}, <<"non-existing-addr">>, Timestamp)
	).

%% Ensure that the check_last_tx function only validates transactions in which
%% last tx field matches that expected within the wallet list.
check_last_tx_test_() ->
	{timeout, 60, fun test_check_last_tx/0}.

check_last_tx_pre_fork_2_5_test_() ->
	ar_test_fork:test_on_fork(height_2_4, infinity, fun test_check_last_tx/0).

test_check_last_tx() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), TX#tx.id),
	TX3 = ar_tx:new(Pub1, ?AR(1), ?AR(300), TX#tx.id),
	SignedTX2 = sign_v1(TX2, Priv2, Pub2),
	SignedTX3 = sign_v1(TX3, Priv3, Pub3),
	WalletList =
		maps:from_list(
			[
				{ar_wallet:to_address(Pub1), {1000, <<>>}},
				{ar_wallet:to_address(Pub2), {2000, TX#tx.id}},
				{ar_wallet:to_address(Pub3), {3000, <<>>}}
			]
		),
	false = check_last_tx(WalletList, SignedTX3),
	true = check_last_tx(WalletList, SignedTX2).

tx_fee_test() ->
	{_, Pub1} = ar_wallet:new(),
	{_, Pub2} = ar_wallet:new(),
	Addr1 = ar_wallet:to_address(Pub1),
	Addr2 = ar_wallet:to_address(Pub2),
	WalletList = maps:from_list([{Addr1, {1000, <<>>}}]),
	Size = 1000,
	Rate = ?USD_TO_AR_INITIAL_RATE,
	Height = ar_fork:height_2_4(),
	Timestamp = os:system_time(seconds),
	TXFee = get_tx_fee(Size, Rate, Height, Timestamp),
	?assertEqual(TXFee, get_tx_fee(Size, Rate, Height, WalletList, Addr1, Timestamp)),
	?assert(get_tx_fee(Size, Rate, Height, WalletList, Addr2, Timestamp) > TXFee).

generate_and_validate_even_chunk_tree_test() ->
	Data = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE * 7),
	lists:map(
		fun(ChallengeLocation) ->
			test_generate_chunk_tree_and_validate_path(Data, ChallengeLocation)
		end,
		[
			0, 1, 10, ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE + 1, 2 * ?DATA_CHUNK_SIZE - 1,
			7 * ?DATA_CHUNK_SIZE - 1
		]
	).

generate_and_validate_uneven_chunk_tree_test() ->
	Data = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE * 4 + 10),
	lists:map(
		fun(ChallengeLocation) ->
			test_generate_chunk_tree_and_validate_path(Data, ChallengeLocation)
		end,
		[
			0, 1, 10, ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE + 1, 2 * ?DATA_CHUNK_SIZE - 1,
			4 * ?DATA_CHUNK_SIZE + 9
		]
	).

test_generate_chunk_tree_and_validate_path(Data, ChallengeLocation) ->
	ChunkStart = ChallengeLocation - ChallengeLocation rem ?DATA_CHUNK_SIZE,
	Chunk = binary:part(Data, ChunkStart, min(?DATA_CHUNK_SIZE, byte_size(Data) - ChunkStart)),
	#tx{ data_root = DataRoot, data_tree = DataTree } =
		ar_tx:generate_chunk_tree(
			#tx {
				data = Data,
				data_size = byte_size(Data)
			}
		),
	DataPath =
		ar_merkle:generate_path(
			DataRoot,
			ChallengeLocation,
			DataTree
		),
	RealChunkID = ar_tx:generate_chunk_id(Chunk),
	{PathChunkID, StartOffset, EndOffset} =
		ar_merkle:validate_path(DataRoot, ChallengeLocation, byte_size(Data), DataPath),
	?assertEqual(RealChunkID, PathChunkID),
	?assert(ChallengeLocation >= StartOffset),
	?assert(ChallengeLocation < EndOffset).
