-module(ar_e2e).

-export([fixture_dir/1, fixture_dir/2, install_fixture/3, load_wallet_fixture/1,
	write_chunk_fixture/3, load_chunk_fixture/2]).

-include_lib("eunit/include/eunit.hrl").

-spec fixture_dir(atom()) -> binary().
fixture_dir(FixtureType) ->
    Dir = filename:dirname(?FILE),
	filename:join([Dir, "fixtures", atom_to_list(FixtureType)]).

-spec fixture_dir(atom(), [binary()]) -> binary().
fixture_dir(FixtureType, SubDirs) ->
    FixtureDir = fixture_dir(FixtureType),
	filename:join([FixtureDir] ++ SubDirs).

-spec install_fixture(binary(), atom(), string()) -> binary().
install_fixture(FilePath, FixtureType, FixtureName) ->
    FixtureDir = fixture_dir(FixtureType),
    ok = filelib:ensure_dir(FixtureDir ++ "/"),
    FixturePath = filename:join([FixtureDir, FixtureName]),
    file:copy(FilePath, FixturePath),
    FixturePath.

-spec load_wallet_fixture(atom()) -> tuple().
load_wallet_fixture(WalletFixture) ->
    ?debugFmt("Loading wallet fixture: ~p", [WalletFixture]),
    WalletName = atom_to_list(WalletFixture),
    FixtureDir = fixture_dir(wallets),
    FixturePath = filename:join([FixtureDir, WalletName ++ ".json"]),
    Wallet = ar_wallet:load_keyfile(FixturePath),
    Address = ar_wallet:to_address(Wallet),
	WalletPath = ar_wallet:wallet_filepath(ar_util:encode(Address)),
    file:copy(FixturePath, WalletPath),
    ar_wallet:load_keyfile(WalletPath).

-spec write_chunk_fixture(binary(), non_neg_integer(), binary()) -> ok.
write_chunk_fixture(Packing, EndOffset, Chunk) ->
    FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
    FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
    file:write_file(FixturePath, Chunk).

-spec load_chunk_fixture(binary(), non_neg_integer()) -> binary().
load_chunk_fixture(Packing, EndOffset) ->
    FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
    FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
    file:read_file(FixturePath).
