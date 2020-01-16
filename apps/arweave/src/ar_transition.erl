-module(ar_transition).
-export([am_i_ready/0]).
-export([generate_checkpoint/0, generate_checkpoint/1]).
-export([save_checkpoint/1, save_checkpoint/2]).
-export([load_checkpoint/0, load_checkpoint/1]).
-include("ar.hrl").
%%% A module for managing the transition from Arweave v1.x to Arweave 2.x.
%%% You can run `ar_transition:am_i_ready().` on the console to see if your
%%% node is prepared for the upgrade.

am_i_ready() ->
    ToGo = ar_fork:height_2_0() - length(load_checkpoint()),
    io:format(
        "During the Arweave 2.0 upgrade you will have to re-verify ~w blocks.~n",
        [ToGo]
    ),
    io:format("In order to lower the amount of work required during the upgrade "
        "please run `ar_transition:generate_checkpoint().`~n"),
    ToGo.

generate_checkpoint() ->
    generate_checkpoint(ar_node:get_block_index(whereis(entrypoint_node))).
generate_checkpoint(BI) ->
    Checkpoint = generate_checkpoint(BI, load_checkpoint()),
    io:format("Generated checkpoint to height ~w. Saving...~n", [length(Checkpoint)]),
    save_checkpoint(Checkpoint),
    Checkpoint.
generate_checkpoint(BI, CP) ->
    lists:reverse(do_generate_checkpoint(lists:reverse(?BI_TO_BHL(BI)), lists:reverse(CP), BI)).

do_generate_checkpoint([], [], _) -> [];
do_generate_checkpoint([_|HL], [CPEntry|CP], BI) ->
    [CPEntry|do_generate_checkpoint(HL, CP, BI)];
do_generate_checkpoint([H|HL], [], BI) ->
    RawB =
        ar_node:get_block(
            ar_bridge:get_remote_peers(whereis(http_bridge_node)),
            H,
            BI
        ),
    case ar_block:verify_indep_hash(RawB) of
        false ->
            io:format("ERROR: Received incorrect block for indep hash: ~p",
                [ar_utils:encode(H)]),
            error;
        true ->
            BWithTree = ar_block:generate_tx_tree(RawB#block { txs = ar_storage:read_tx(RawB#block.txs) }),
            B = BWithTree#block { header_hash = ar_weave:header_hash(BWithTree) },
            ar_storage:write_block(B),
            [{B#block.header_hash, B#block.weave_size}|do_generate_checkpoint(HL, [], BI)]
    end.

save_checkpoint(Checkpoint) ->
    save_checkpoint(ar_meta_db:get(checkpoint_location), Checkpoint).
save_checkpoint(File, Checkpoint) ->
    JSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(Checkpoint)),
    file:write_file(File, JSON).

-ifdef(DEBUG).
load_checkpoint() -> [].
load_checkpoint(_) -> [].
-else.
load_checkpoint() ->
    load_checkpoint(ar_meta_db:get(checkpoint_location)).
load_checkpoint(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            CP = ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Bin)),
            ar:info(
                [
                    loaded_header_hash_block_index,
                    {file, File},
                    {cp, CP}
                ]
            ),
            CP;
        _ ->
            io:format("Checkpoint not loaded. Starting from Genesis block..."),
            []
    end.
-endif.
