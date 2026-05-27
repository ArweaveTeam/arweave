%%% @doc Integration tests for `handle_set' value transforms on real
%%% option specs. Boots the full arweave_config application so the
%%% production spec contributors (and their `handle_set' callbacks)
%%% are registered.
-module(arweave_config_options_handle_set_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		mining_address_base64,
		start_from_block_base64,
		verify_mode_atoms_and_binaries
	].

%%====================================================================
%% Test cases
%%====================================================================

mining_address_base64(_Config) ->
	arweave_config:with_test_config(fun() ->
		Raw = <<0:256>>,
		Encoded = b64fast:encode(Raw),
		?assertMatch({ok, Raw}, arweave_config:set([mining, address], Encoded)),
		?assertEqual(Raw, arweave_config:get([mining, address])),

		%% A 16-byte payload base64-encoded is the wrong size.
		BadEncoded = b64fast:encode(<<0:128>>),
		?assertMatch({error, _},
			arweave_config:set([mining, address], BadEncoded))
	end),
	ok.

start_from_block_base64(_Config) ->
	arweave_config:with_test_config(fun() ->
		Raw = <<0:384>>,
		Encoded = b64fast:encode(Raw),
		?assertMatch({ok, Raw},
			arweave_config:set([join, start_from_block], Encoded)),
		?assertEqual(Raw, arweave_config:get([join, start_from_block])),

		%% A 32-byte payload base64-encoded is the wrong size for
		%% `start_from_block`.
		BadEncoded = b64fast:encode(<<0:256>>),
		?assertMatch({error, _},
			arweave_config:set([join, start_from_block], BadEncoded))
	end),
	ok.

verify_mode_atoms_and_binaries(_Config) ->
	arweave_config:with_test_config(fun() ->
		?assertMatch({ok, false}, arweave_config:set([verify, mode], false)),
		?assertEqual(false, arweave_config:get([verify, mode])),

		?assertMatch({ok, purge}, arweave_config:set([verify, mode], purge)),
		?assertEqual(purge, arweave_config:get([verify, mode])),

		?assertMatch({ok, log}, arweave_config:set([verify, mode], log)),
		?assertEqual(log, arweave_config:get([verify, mode])),

		?assertMatch({ok, purge},
			arweave_config:set([verify, mode], <<"purge">>)),
		?assertEqual(purge, arweave_config:get([verify, mode])),

		?assertMatch({ok, log},
			arweave_config:set([verify, mode], <<"log">>)),
		?assertEqual(log, arweave_config:get([verify, mode])),

		?assertMatch({error, _},
			arweave_config:set([verify, mode], <<"bogus">>))
	end),
	ok.
