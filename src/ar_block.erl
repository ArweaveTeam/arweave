-module(ar_block).
-export([new/0]).
-export([block_to_binary/1, block_field_size_limit/1, generate_block_data_segment/6]).
-export([verify_dep_hash/4, verify_indep_hash/1]).
-export([encrypt_block/2,decrypt_block/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

new() ->
	#block {
        nonce = crypto:strong_rand_bytes(8),
        hash = crypto:strong_rand_bytes(32),
        indep_hash = crypto:strong_rand_bytes(32),
        hash_list = []
    }.

encrypt_block(B, R) ->
    Recall = block_to_binary(R),
    Hash = B#block.hash,
    Nonce = binary:part(Hash, 0, 16),
    Key = crypto:hash(?HASH_ALG,<<Hash/binary, Recall/binary>>),
    PlainText = pad_to_length(block_to_binary(R)),
    %% block to binary
    %% pad binary to multiple of block
    CiperText = 
        crypto:block_encrypt(
            aes_cbc,
            Key,
            Nonce,
            PlainText
        ),
    {Key, CiperText}.
decrypt_block(B, CipherText, Key) ->
    Nonce = binary:part(B#block.hash, 0, 16),
    PlainText = 
        crypto:block_decrypt(
            aes_cbc,
            Key,
            Nonce,
            CipherText
        ).

%% @doc Generate a hashable binary from a #block object.
pad_to_length(Binary) ->
    Pad = (32 - ((bit_size(Binary)+1) rem 32)),
    <<Binary/binary, 1, 0:(Pad*8)>>.

unpad_binary(Binary) ->
    ar_util:rev_bin(do_unpad_binary(ar_util:rev_bin(Binary))).
do_unpad_binary(Binary) ->
    case Binary of
        <<0, Rest/bitstring >> -> do_unpad_binary(Rest);
        <<1, Rest/bitstring >> -> Rest
    end.

block_to_binary(B) ->
	<<
		(B#block.nonce)/binary,
        (B#block.previous_block)/binary,
        (list_to_binary(integer_to_list(B#block.timestamp)))/binary,
		(list_to_binary(integer_to_list(B#block.last_retarget)))/binary,
		(list_to_binary(integer_to_list(B#block.diff)))/binary,
        (list_to_binary(integer_to_list(B#block.height)))/binary,
        (B#block.hash)/binary,
        (B#block.indep_hash)/binary,
        (
            binary:list_to_bin(
                lists:map(
                    fun ar_tx:to_binary/1,
                    lists:sort(ar_storage:read_tx(B#block.txs))
                )
            )
        )/binary,
        (list_to_binary(B#block.hash_list))/binary,
        (
            binary:list_to_bin(
                lists:map(
                    fun ar_wallet:to_binary/1,
                    B#block.wallet_list
                )
            )
        )/binary,
        (
            case is_atom(B#block.reward_addr) of
                true -> <<>>;
                false -> B#block.reward_addr
            end
        )/binary,
        (list_to_binary(B#block.tags))/binary
	>>.

%% @doc Given a block checks that the lengths conform to the specified limits.
block_field_size_limit(B = #block { reward_addr = unclaimed }) ->
    block_field_size_limit(B#block { reward_addr = <<>> });
block_field_size_limit(B) ->
	(byte_size(B#block.nonce) =< 8) and
    (byte_size(B#block.previous_block) =< 32) and
	(byte_size(integer_to_binary(B#block.timestamp)) =< 12) and
    (byte_size(integer_to_binary(B#block.last_retarget)) =< 12) and
    (byte_size(integer_to_binary(B#block.diff)) =< 10) and
    (byte_size(integer_to_binary(B#block.height)) =< 20) and
    (byte_size(B#block.hash) =< 32) and
    (byte_size(B#block.indep_hash) =< 32) and
    (byte_size(B#block.reward_addr) =< 32) and
    (byte_size(list_to_binary(B#block.tags)) =< 2048).

%% @docs Generate a hashable data segment for a block from the current
%% block, recall block, TXs to be mined, reward address and tags.
generate_block_data_segment(CurrentB, RecallB, [unavailable], RewardAddr, Timestamp, Tags) ->
    generate_block_data_segment(CurrentB, RecallB, [], RewardAddr, Timestamp, Tags);
generate_block_data_segment(CurrentB, RecallB, TXs, unclaimed, Timestamp, Tags) ->
    generate_block_data_segment(CurrentB, RecallB, TXs, <<>>, Timestamp, Tags);
generate_block_data_segment(CurrentB, RecallB, TXs, RewardAddr, Timestamp, Tags) ->
    Retarget = case ar_retarget:is_retarget_height(CurrentB#block.height + 1) of
        true -> Timestamp;
        false -> CurrentB#block.last_retarget
    end,
    case RewardAddr == undefined of
        true ->
            NewWalletList =
                ar_node:apply_mining_reward(
                    ar_node:apply_txs(CurrentB#block.wallet_list, TXs),
                    RewardAddr,
                    TXs,
                    length(CurrentB#block.hash_list) - 1
                    );
        false ->
            NewWalletList =
                ar_node:apply_txs(CurrentB#block.wallet_list, TXs)
    end,
    % ar:d({indep, CurrentB#block.indep_hash}),
    % ar:d({retarget, integer_to_binary(Retarget)}),
    % ar:d({height, integer_to_binary(CurrentB#block.height + 1)}),
    % ar:d({wallets, binary:list_to_bin(lists:map(fun ar_wallet:to_binary/1, NewWalletList))}),
    % ar:d({reward, case is_atom(RewardAddr) of true -> <<>>; false -> RewardAddr end}),
    % ar:d({tags, list_to_binary(Tags)}),
    % ar:d({recall, block_to_binary(RecallB)}),
    % ar:d({txs, binary:list_to_bin(lists:map(fun ar_tx:to_binary/1, TXs))}),
    <<
        (CurrentB#block.indep_hash)/binary,
        (CurrentB#block.hash)/binary,
        (integer_to_binary(Timestamp))/binary,
        (integer_to_binary(Retarget))/binary,
        (integer_to_binary(CurrentB#block.height + 1))/binary,
        (list_to_binary([CurrentB#block.indep_hash | CurrentB#block.hash_list]))/binary,
        (
            binary:list_to_bin(
                lists:map(
                    fun ar_wallet:to_binary/1,
                    NewWalletList
                )
            )
        )/binary,
        (
            case is_atom(RewardAddr) of
                true -> <<>>;
                false -> RewardAddr
            end
        )/binary,
        (list_to_binary(Tags))/binary,
        (block_to_binary(RecallB))/binary,
        (
            binary:list_to_bin(
                lists:map(
                    fun ar_tx:to_binary/1,
                    TXs
                )
            )
        )/binary
    >>.

verify_indep_hash(Block = #block { indep_hash = Indep }) ->
    Indep == ar_weave:indep_hash(Block).

verify_dep_hash(NewB, OldB, RecallB, MinedTXs) ->
    NewB#block.hash ==
        ar_weave:hash(
            ar_block:generate_block_data_segment(
                OldB,
                RecallB,
                MinedTXs,
                NewB#block.reward_addr,
                NewB#block.timestamp,
                NewB#block.tags
            ),
            NewB#block.nonce
        ).

pad_unpad_roundtrip_test() ->
    Pad = pad_to_length(<<"abcdefghabcdefghabcd">>),
    UnPad = unpad_binary(Pad).

encrypt_decrypt_block_test() ->
    B0 = ar_weave:init([]),
    ar_storage:write_block(B0),
    B1 = ar_weave:add(B0, []),
    {Key, CiperText, Pad} = encrypt_block(hd(B1), hd(B0)).