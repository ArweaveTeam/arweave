-module(arweave_config_help_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
	[{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

all() ->
	[
		default_help_lists_groups,
		group_form_prints_group_detail,
		hidden_options_left_out,
		runtime_marker_and_legend
	].

%%====================================================================
%% Test cases
%%====================================================================

default_help_lists_groups(_Config) ->
	Output = capture(fun arweave_config_help:print/0),
	?assertNotEqual(nomatch, binary:match(Output, <<"=== peers ===">>)),
	?assertNotEqual(nomatch, binary:match(Output, <<"=== gossip ===">>)),
	ok.

%% The per-group form must resolve group names that exist. (The CLI
%% runs help under a minimal boot where no spec module is preloaded —
%% scripts/smoke_cli.sh covers that path end to end; this case guards
%% the lookup logic itself.)
group_form_prints_group_detail(_Config) ->
	Output = capture(fun() -> arweave_config_help:print_group("peers") end),
	?assertNotEqual(nomatch, binary:match(Output, <<"peers.trusted">>)),
	ok.

%% Options marked `hidden => true' (the gated config.http.* group)
%% stay registered but must not render anywhere in help.
hidden_options_left_out(_Config) ->
	Output = capture(fun arweave_config_help:print/0),
	?assertEqual(nomatch, binary:match(Output, <<"config.http">>)),
	?assertEqual(nomatch, binary:match(Output, <<"=== config ===">>)),
	ok.

%% Summary layout: runtime-writable options carry a `*' marker
%% directly before the key, and a legend explains it once.
runtime_marker_and_legend(_Config) ->
	Output = capture(fun arweave_config_help:print/0),
	?assertNotEqual(nomatch, binary:match(Output, <<"* peers.block_gossip">>)),
	?assertNotEqual(nomatch,
		binary:match(Output, <<"(* = settable at runtime via `config set`)">>)),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

capture(Fun) ->
	ct:capture_start(),
	Fun(),
	ct:capture_stop(),
	%% characters_to_binary, not iolist_to_binary: the output contains
	%% codepoints above Latin-1 (the `…' truncation marker).
	unicode:characters_to_binary(ct:capture_get()).
