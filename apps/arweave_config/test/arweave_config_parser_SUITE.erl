%%% @doc Tests for `arweave_config_parser` — parsing and formatting of
%%% option_key lists.
-module(arweave_config_parser_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
	ok = arweave_config:start(),
	Config.

end_per_suite(_Config) ->
	ok = arweave_config:stop().

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(_TestCase, _Config) -> ok.

all() ->
	[
		key_atom_list,
		key_dotted_string,
		key_dotted_binary,
		key_with_bracketed_segment,
		key_invalid,
		format_key_basic,
		format_key_wildcards,
		format_key_mixed_types,
		is_parameter_yes_no,
		key_format_roundtrip
	].

%%====================================================================
%% Test cases
%%====================================================================

%% --------------------------------------------------------------------
%% key/1 tests
%% --------------------------------------------------------------------

key_atom_list(_Config) ->
	?assertEqual({ok, [global, debug]},
		arweave_config_parser:key([global, debug])),
	?assertEqual({ok, [storage, 3, unpacked, state]},
		arweave_config_parser:key([storage, 3, unpacked, state])),
	ok.

key_dotted_string(_Config) ->
	?assertEqual({ok, [global, debug]},
		arweave_config_parser:key("global.debug")),
	%% Leading separator is also accepted.
	?assertEqual({ok, [global, debug]},
		arweave_config_parser:key(".global.debug")),
	?assertEqual({ok, [storage, 3, unpacked, state]},
		arweave_config_parser:key("storage.3.unpacked.state")),
	ok.

key_dotted_binary(_Config) ->
	?assertEqual({ok, [global, debug]},
		arweave_config_parser:key(<<"global.debug">>)),
	?assertEqual({ok, [storage, 3, unpacked, state]},
		arweave_config_parser:key(<<"storage.3.unpacked.state">>)),
	ok.

key_with_bracketed_segment(_Config) ->
	?assertEqual(
		{ok, [peers, <<"127.0.0.1:1984">>, trusted]},
		arweave_config_parser:key("peers.[127.0.0.1:1984].trusted")),
	?assertEqual(
		{ok, [peers, <<"127.0.0.1:1984">>, trusted]},
		arweave_config_parser:key(<<"peers.[127.0.0.1:1984].trusted">>)),
	ok.

key_invalid(_Config) ->
	%% Empty string -> empty_key.
	?assertMatch({error, #{ reason := empty_key }},
		arweave_config_parser:key("")),
	%% Bad char -> bad_char.
	?assertMatch({error, #{ reason := bad_char }},
		arweave_config_parser:key(<<"~">>)),
	%% Double separator -> multi_separators.
	?assertMatch({error, #{ reason := multi_separators }},
		arweave_config_parser:key(<<"global..debug">>)),
	%% Trailing separator -> separator_ending.
	?assertMatch({error, #{ reason := separator_ending }},
		arweave_config_parser:key(<<"test.">>)),
	%% Unknown atom name -> invalid_key.
	?assertMatch({error, #{ reason := invalid_key }},
		arweave_config_parser:key("totally_random_key_xyz")),
	ok.

%% --------------------------------------------------------------------
%% format_key/1 tests
%% --------------------------------------------------------------------

format_key_basic(_Config) ->
	?assertEqual(<<"global.debug">>,
		arweave_config_parser:format_key([global, debug])),
	?assertEqual(<<"storage.3.unpacked.state">>,
		arweave_config_parser:format_key([storage, 3, unpacked, state])),
	?assertEqual(<<>>, arweave_config_parser:format_key([])),
	ok.

format_key_wildcards(_Config) ->
	?assertEqual(<<"peers.<peer_id>.trusted">>,
		arweave_config_parser:format_key([peers, {peer_id}, trusted])),
	?assertEqual(<<"<root>">>,
		arweave_config_parser:format_key([{root}])),
	ok.

format_key_mixed_types(_Config) ->
	%% atom
	?assertEqual(<<"foo">>, arweave_config_parser:format_segment(foo)),
	%% integer
	?assertEqual(<<"42">>, arweave_config_parser:format_segment(42)),
	%% binary
	?assertEqual(<<"bar">>, arweave_config_parser:format_segment(<<"bar">>)),
	%% list (charlist)
	?assertEqual(<<"baz">>, arweave_config_parser:format_segment("baz")),
	%% wildcard
	?assertEqual(<<"<wild>">>, arweave_config_parser:format_segment({wild})),

	%% Combined into a key.
	?assertEqual(<<"foo.42.bar.baz.<wild>">>,
		arweave_config_parser:format_key(
			[foo, 42, <<"bar">>, "baz", {wild}])),
	ok.

%% --------------------------------------------------------------------
%% is_parameter/1 tests
%% --------------------------------------------------------------------

is_parameter_yes_no(_Config) ->
	%% Already-parsed lists.
	?assert(arweave_config_parser:is_parameter([global, debug])),
	?assert(arweave_config_parser:is_parameter([storage, 3, unpacked, state])),
	?assert(arweave_config_parser:is_parameter(
		[peers, <<"127.0.0.1:1984">>, trusted])),

	%% Raw inputs.
	?assertNot(arweave_config_parser:is_parameter("rocksdb.flush_interval")),
	?assertNot(arweave_config_parser:is_parameter(<<"global.debug">>)),
	?assertNot(arweave_config_parser:is_parameter(global)),
	?assertNot(arweave_config_parser:is_parameter([])),
	%% A list containing an empty binary is not a valid parsed key.
	?assertNot(arweave_config_parser:is_parameter([<<>>, debug])),
	ok.

%% --------------------------------------------------------------------
%% Round-trip
%% --------------------------------------------------------------------

key_format_roundtrip(_Config) ->
	Samples = [
		<<"global.debug">>,
		<<"storage.3.unpacked.state">>,
		<<"peers.[127.0.0.1:1984].trusted">>
	],
	lists:foreach(
		fun(Input) ->
			{ok, Parsed} = arweave_config_parser:key(Input),
			Formatted = arweave_config_parser:format_key(Parsed),
			%% Bracketed segments format as the bare binary value,
			%% not re-wrapped in `[...]`, so normalize the input for
			%% the comparison.
			Expected = binary:replace(
				binary:replace(Input, <<"[">>, <<>>, [global]),
				<<"]">>, <<>>, [global]),
			?assertEqual(Expected, Formatted)
		end, Samples),
	ok.
