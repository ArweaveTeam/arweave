%%% @doc Targeted coverage for logging/debug options that are skipped
%%% by the generic spec-driven format sweep.
-module(arweave_config_options_logging_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	%% Ensure predictable logger handler state.
	_ = safe_stop_handler(arweave_debug),
	_ = safe_stop_handler(arweave_info),
	_ = safe_stop_handler(arweave_http_api),
	Config.

end_per_testcase(_TestCase, _Config) ->
	_ = safe_stop_handler(arweave_debug),
	_ = safe_stop_handler(arweave_info),
	_ = safe_stop_handler(arweave_http_api),
	ok = arweave_config:stop().

all() ->
	[
		debug_toggle_updates_store_and_handler,
		logging_path_coerces_to_list,
		logging_formatter_and_limits_update,
		logger_set_with_no_live_handler_stores_anyway,
		debug_handler_toggle_via_logging_spec,
		debug_options_collision,
		http_api_handler_toggle
	].

%%====================================================================
%% Test cases
%%====================================================================

debug_toggle_updates_store_and_handler(_Config) ->
	?assertEqual(false, arweave_config:get([debug])),
	{ok, true} = arweave_config:set([debug], true),
	?assertEqual(true, arweave_config:get([debug])),
	?assertMatch({ok, _}, logger:get_handler_config(arweave_debug)),
	{ok, false} = arweave_config:set([debug], false),
	?assertEqual(false, arweave_config:get([debug])),
	?assertMatch({error, _}, logger:get_handler_config(arweave_debug)),
	ok.

logging_path_coerces_to_list(_Config) ->
	{ok, "/tmp/arweave-logs"} =
		arweave_config:set([logging, path], <<"/tmp/arweave-logs">>),
	?assertEqual("/tmp/arweave-logs", arweave_config:get([logging, path])),
	ok.

logging_formatter_and_limits_update(_Config) ->
	%% Create handler so logger_set/4 updates live logger config.
	ok = ar_logger:start_handler(arweave_info),
	{ok, 9001} = arweave_config:set([logging, formatter, max_size], 9001),
	{ok, 128} = arweave_config:set([logging, formatter, depth], 128),
	{ok, 4096} = arweave_config:set([logging, formatter, chars_limit], 4096),
	{ok, 25} = arweave_config:set([logging, max_no_files], 25),
	{ok, 1048576} = arweave_config:set([logging, max_no_bytes], 1048576),
	?assertEqual(9001, arweave_config:get([logging, formatter, max_size])),
	?assertEqual(128, arweave_config:get([logging, formatter, depth])),
	?assertEqual(4096, arweave_config:get([logging, formatter, chars_limit])),
	?assertEqual(25, arweave_config:get([logging, max_no_files])),
	?assertEqual(1048576, arweave_config:get([logging, max_no_bytes])),
	{ok, #{formatter := {logger_formatter, FormatterCfg},
		config := HandlerCfg}} = logger:get_handler_config(arweave_info),
	#{max_size := 9001, depth := 128, chars_limit := 4096} = FormatterCfg,
	#{max_no_files := 25, max_no_bytes := 1048576} = HandlerCfg,
	ok.

%% logger_set/4 falls through to {store, Value} when the target
%% handler isn't running — the option is recorded for when it does.
logger_set_with_no_live_handler_stores_anyway(_Config) ->
	?assertMatch({error, _}, logger:get_handler_config(arweave_info)),
	{ok, 250} = arweave_config:set([logging, drop_mode_qlen], 250),
	?assertEqual(250, arweave_config:get([logging, drop_mode_qlen])),
	ok.

%% The handle_set closure on [logging, handlers, debug] starts/stops
%% the arweave_debug logger handler. Distinct from the [debug] option
%% in arweave_config_options_misc (see debug_options_collision).
debug_handler_toggle_via_logging_spec(_Config) ->
	?assertMatch({error, _}, logger:get_handler_config(arweave_debug)),
	{ok, true} = arweave_config:set([logging, handlers, debug], true),
	?assertMatch({ok, _}, logger:get_handler_config(arweave_debug)),
	{ok, false} = arweave_config:set([logging, handlers, debug], false),
	?assertMatch({error, _}, logger:get_handler_config(arweave_debug)),
	ok.

%% [debug] and [logging, handlers, debug] independently affect the
%% same arweave_debug handler. Toggling one to false stops the handler
%% even when the other is stored as true — they share no state.
debug_options_collision(_Config) ->
	{ok, true} = arweave_config:set([debug], true),
	?assertMatch({ok, _}, logger:get_handler_config(arweave_debug)),
	{ok, true} = arweave_config:set([logging, handlers, debug], true),
	{ok, false} = arweave_config:set([logging, handlers, debug], false),
	?assertMatch({error, _}, logger:get_handler_config(arweave_debug)),
	?assertEqual(true, arweave_config:get([debug])),
	ok.

http_api_handler_toggle(_Config) ->
	?assertMatch({error, _}, logger:get_handler_config(arweave_http_api)),
	{ok, true} = arweave_config:set([logging, handlers, http, api], true),
	?assertMatch({ok, _}, logger:get_handler_config(arweave_http_api)),
	{ok, false} = arweave_config:set([logging, handlers, http, api], false),
	?assertMatch({error, _}, logger:get_handler_config(arweave_http_api)),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

safe_stop_handler(Handler) ->
	case logger:get_handler_config(Handler) of
		{ok, _} -> ar_logger:stop_handler(Handler);
		_ -> ok
	end.
