-module(ar_cleanup).
-export([remove_invalid_blocks/1, remove_invalid_txs/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BLOCK_DIR, "blocks/").
-define(BLOCK_ENC_DIR, "blocks/enc").
%%% Functions to clean up blocks not on the list of valid blocks
%%% And invalid transactions that are no longer valid for some reason



%% @doc Remove all blocks from blocks directory not in HashList
remove_invalid_blocks(HashList) ->
    {ok, RawFiles} = file:list_dir(?BLOCK_DIR),
    % TODO: make less awful
    Files =
        lists:filter(
            fun(X) ->
                case X of
                    "enc" -> false;
                    _ -> true
                end
            end,
            RawFiles
        ),
    lists:foreach(
        fun(X) ->
            file:delete(?BLOCK_DIR ++ X)
        end,
        lists:filter(
            fun(Y) ->
                case lists:foldl(
                    fun(Z, Sum) -> Sum + string:str(Y, ar_util:encode(Z)) end,
                    0,
                    HashList
                ) of
                    0 -> true;
                    _ -> false
                end
            end,
            Files
        )
    ),
    {ok, FilesEnc} = file:list_dir(?BLOCK_ENC_DIR),
    lists:foreach(
        fun(X) ->
            file:delete(?BLOCK_ENC_DIR ++ X)
        end,
        FilesEnc
    ).
    
%% @doc Remove all TXs from the TX directory that are "too cheap"
remove_invalid_txs(Diff) ->
    ok.

%% @doc test that blocks are correctly removed
remove_invalid_blocks_test() ->
    ok.

%% @doc test that txs are correctly removed
remove_invalid_txs_test() ->
    ok.