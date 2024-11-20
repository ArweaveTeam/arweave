-module(ar_e2e).

-export([fixture_dir/1, install_fixture/3, load_wallet_fixture/1]).

-include_lib("eunit/include/eunit.hrl").

-spec fixture_dir(atom()) -> binary().
fixture_dir(FixtureType) ->
    Dir = filename:dirname(?FILE),
	filename:join([Dir, "fixtures", atom_to_list(FixtureType)]).

-spec install_fixture(binary(), atom(), string()) -> binary().
install_fixture(FilePath, FixtureType, FixtureName) ->
    FixtureDir = fixture_dir(FixtureType),
    ?debugFmt("Fixture dir: ~p", [FixtureDir]),
    ok = filelib:ensure_dir(FixtureDir ++ "/"),
    FixturePath = fixture_path(FixtureType, FixtureName),
    ?debugFmt("Fixture path: ~p", [FixturePath]),
    file:copy(FilePath, FixturePath),
    FixturePath.

-spec load_wallet_fixture(atom()) -> tuple().
load_wallet_fixture(WalletFixture) ->
    ?debugFmt("Loading wallet fixture: ~p", [WalletFixture]),
    WalletName = atom_to_list(WalletFixture),
    FixturePath = fixture_path(wallets, WalletName ++ ".json"),
    Wallet = ar_wallet:load_keyfile(FixturePath),
    Address = ar_wallet:to_address(Wallet),
	WalletPath = ar_wallet:wallet_filepath(ar_util:encode(Address)),
    file:copy(FixturePath, WalletPath),
    ar_wallet:load_keyfile(WalletPath).


%% --------------------------------------------------------------------------------------------
%% Private functions
%% --------------------------------------------------------------------------------------------


fixture_path(FixtureType, FixtureName) ->
    filename:join(fixture_dir(FixtureType), FixtureName).
