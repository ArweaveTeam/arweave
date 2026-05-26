%%% @doc Specs for the `logging` option group. Options for
%%% shaping how node logs are emitted and protected under load.
-module(arweave_config_options_logging).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

%% Defaults shared between the top-level [logging, X] specs and the
%% per-handler [logging, handlers, _, X] specs. Single source of
%% truth so updating a parent's default sweeps its mirror children.
-define(LOG_FORMATTER_TEMPLATE,
	[time," [",level,"] ",mfa,":",line," ",msg,"\n"]).
-define(LOG_FORMATTER_MAX_SIZE, 8128).
-define(LOG_FORMATTER_DEPTH, 256).
-define(LOG_FORMATTER_CHARS_LIMIT, 16256).
-define(LOG_MAX_NO_FILES, 20).
-define(LOG_MAX_NO_BYTES, 51418800).
-define(LOG_COMPRESS_ON_ROTATE, false).
-define(LOG_SYNC_MODE_QLEN, 10).
-define(LOG_DROP_MODE_QLEN, 200).
-define(LOG_FLUSH_QLEN, 1000).
-define(LOG_BURST_LIMIT_ENABLE, true).
-define(LOG_BURST_LIMIT_MAX_COUNT, 500).
-define(LOG_BURST_LIMIT_WINDOW_TIME, 1000).
-define(LOG_OVERLOAD_KILL_ENABLE, true).
-define(LOG_OVERLOAD_KILL_QLEN, 20_000).
-define(LOG_OVERLOAD_KILL_MEM_SIZE, 3_000_000).
-define(LOG_OVERLOAD_KILL_RESTART_AFTER, 5000).

%% Short descriptions, shared between parent specs and their
%% per-handler mirrors. Help output shows the same one-liner for both.
-define(LOG_DESC_FORMATTER_TEMPLATE,
	<<"Configure the default logging formatter template.">>).
-define(LOG_DESC_FORMATTER_MAX_SIZE,
	<<"Maximum size in bytes of a single formatted log entry.">>).
-define(LOG_DESC_FORMATTER_DEPTH,
	<<"Maximum nesting depth when printing terms in log entries.">>).
-define(LOG_DESC_FORMATTER_CHARS_LIMIT,
	<<"Maximum number of characters in a single formatted log entry.">>).
-define(LOG_DESC_MAX_NO_FILES,
	<<"Maximum number of rotated log files to retain.">>).
-define(LOG_DESC_MAX_NO_BYTES,
	<<"Maximum size in bytes of each log file before rotation.">>).
-define(LOG_DESC_COMPRESS_ON_ROTATE,
	<<"Compress rotated log files with gzip.">>).
-define(LOG_DESC_SYNC_MODE_QLEN,
	<<"Queue length above which the logger handler switches to "
	  "synchronous mode.">>).
-define(LOG_DESC_DROP_MODE_QLEN,
	<<"Queue length above which the logger handler drops "
	  "low-priority messages.">>).
-define(LOG_DESC_FLUSH_QLEN,
	<<"Queue length above which the logger handler flushes its "
	  "message queue.">>).
-define(LOG_DESC_BURST_LIMIT_ENABLE,
	<<"Enable burst-limit protection on the logger handler.">>).
-define(LOG_DESC_BURST_LIMIT_MAX_COUNT,
	<<"Maximum number of messages allowed within the burst-limit "
	  "window.">>).
-define(LOG_DESC_BURST_LIMIT_WINDOW_TIME,
	<<"Burst-limit sliding window in milliseconds.">>).
-define(LOG_DESC_OVERLOAD_KILL_ENABLE,
	<<"Kill the logger handler when it becomes overloaded.">>).
-define(LOG_DESC_OVERLOAD_KILL_QLEN,
	<<"Queue length that triggers overload-kill of the logger "
	  "handler.">>).
-define(LOG_DESC_OVERLOAD_KILL_MEM_SIZE,
	<<"Memory usage in bytes that triggers overload-kill of the "
	  "logger handler.">>).
-define(LOG_DESC_OVERLOAD_KILL_RESTART_AFTER,
	<<"Milliseconds to wait before restarting a logger handler that "
	  "was killed for overload.">>).

specs() ->
	[
		#{
			enabled => true,
			option_key => [logging,formatter,template],
			default => ?LOG_FORMATTER_TEMPLATE,
			type => logging_template,
			runtime => false,
			short_description => ?LOG_DESC_FORMATTER_TEMPLATE
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger.html
			%% Stored as a string (list of integer); the `path` type
			%% always returns a binary, so the handler coerces it back
			%% to a list for the logger handler API.
			option_key => [logging,path],
			default => "./logs",
			type => path,
			runtime => false,
			short_description => <<"Set the directory used for Arweave logs.">>,
			handle_set => fun(_K, Path, _S, _) when is_binary(Path) ->
				{store, binary_to_list(Path)}
			end
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			option_key => [logging,formatter,max_size],
			default => ?LOG_FORMATTER_MAX_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_MAX_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,max_size]
			}
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			option_key => [logging,formatter,depth],
			default => ?LOG_FORMATTER_DEPTH,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_DEPTH,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,depth]
			}
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			option_key => [logging,formatter,chars_limit],
			default => ?LOG_FORMATTER_CHARS_LIMIT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_CHARS_LIMIT,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			option_key => [logging,max_no_files],
			default => ?LOG_MAX_NO_FILES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_FILES,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,max_no_files]
			}
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			option_key => [logging,max_no_bytes],
			default => ?LOG_MAX_NO_BYTES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_BYTES,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			%% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			option_key => [logging,compress_on_rotate],
			default => ?LOG_COMPRESS_ON_ROTATE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_COMPRESS_ON_ROTATE,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			option_key => [logging,sync_mode_qlen],
			default => ?LOG_SYNC_MODE_QLEN,
			type => pos_integer,
			runtime => false,
			short_description => ?LOG_DESC_SYNC_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,drop_mode_qlen],
			default => ?LOG_DROP_MODE_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_DROP_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,flush_qlen],
			default => ?LOG_FLUSH_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FLUSH_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,burst_limit_enable],
			default => ?LOG_BURST_LIMIT_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,burst_limit_max_count],
			default => ?LOG_BURST_LIMIT_MAX_COUNT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_MAX_COUNT,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			option_key => [logging,burst_limit_window_time],
			default => ?LOG_BURST_LIMIT_WINDOW_TIME,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_WINDOW_TIME,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			option_key => [logging,overload_kill_enable],
			default => ?LOG_OVERLOAD_KILL_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,overload_kill_qlen],
			default => ?LOG_OVERLOAD_KILL_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,overload_kill_mem_size],
			default => ?LOG_OVERLOAD_KILL_MEM_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_MEM_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			option_key => [logging,overload_kill_restart_after],
			default => ?LOG_OVERLOAD_KILL_RESTART_AFTER,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_RESTART_AFTER,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_restart_after]
			}
		},

		%% debug logs
		#{
			enabled => true,
			option_key => [logging,handlers,debug],
			default => false,
			type => boolean,
			runtime => true,
			short_description =>
				<<"Enable the debug-level logger handler.">>,
			handle_set => fun
				(_,true,_,_) ->
					ar_logger:start_handler(arweave_debug),
					{store, true};
				(_,false,_,_) ->
					ar_logger:stop_handler(arweave_debug),
					{store, false}
			end
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,compress_on_rotate],
			default => ?LOG_COMPRESS_ON_ROTATE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_COMPRESS_ON_ROTATE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,max_no_files],
			default => ?LOG_MAX_NO_FILES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_FILES,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,max_no_files]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,max_no_bytes],
			default => ?LOG_MAX_NO_BYTES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_BYTES,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,sync_mode_qlen],
			default => ?LOG_SYNC_MODE_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_SYNC_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,drop_mode_qlen],
			default => ?LOG_DROP_MODE_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_DROP_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,flush_qlen],
			default => ?LOG_FLUSH_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FLUSH_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,burst_limit_enable],
			default => ?LOG_BURST_LIMIT_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,burst_limit_max_count],
			default => ?LOG_BURST_LIMIT_MAX_COUNT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_MAX_COUNT,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,burst_limit_window_time],
			default => ?LOG_BURST_LIMIT_WINDOW_TIME,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_WINDOW_TIME,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,overload_kill_enable],
			default => ?LOG_OVERLOAD_KILL_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,overload_kill_qlen],
			default => ?LOG_OVERLOAD_KILL_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,overload_kill_mem_size],
			default => ?LOG_OVERLOAD_KILL_MEM_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_MEM_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,overload_kill_restart_after],
			default => ?LOG_OVERLOAD_KILL_RESTART_AFTER,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_RESTART_AFTER,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_restart_after]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,formatter,chars_limit],
			default => ?LOG_FORMATTER_CHARS_LIMIT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_CHARS_LIMIT,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,formatter,depth],
			default => ?LOG_FORMATTER_DEPTH,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_DEPTH,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,depth]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,formatter,max_size],
			default => ?LOG_FORMATTER_MAX_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_MAX_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,max_size]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,debug,formatter,template],
			default => ?LOG_FORMATTER_TEMPLATE,
			type => logging_template,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_TEMPLATE,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,template]
			}
		},

		%% http api logs
		#{
			enabled => true,
			option_key => [logging,handlers,http,api],
			default => false,
			type => boolean,
			runtime => true,
			short_description =>
				<<"Enable the HTTP API logger handler.">>,
			handle_set => fun
				(_K,true,_S,_) ->
					ar_logger:start_handler(arweave_http_api),
					{store, true};
				(_K,false,_S,_) ->
					ar_logger:stop_handler(arweave_http_api),
					{store, false}
			end
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,compress_on_rotate],
			default => ?LOG_COMPRESS_ON_ROTATE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_COMPRESS_ON_ROTATE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,max_no_files],
			default => ?LOG_MAX_NO_FILES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_FILES,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,max_no_files]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,max_no_bytes],
			default => ?LOG_MAX_NO_BYTES,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_MAX_NO_BYTES,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,sync_mode_qlen],
			default => ?LOG_SYNC_MODE_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_SYNC_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,drop_mode_qlen],
			default => ?LOG_DROP_MODE_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_DROP_MODE_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,flush_qlen],
			default => ?LOG_FLUSH_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FLUSH_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,burst_limit_enable],
			default => ?LOG_BURST_LIMIT_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,burst_limit_max_count],
			default => ?LOG_BURST_LIMIT_MAX_COUNT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_MAX_COUNT,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,burst_limit_window_time],
			default => ?LOG_BURST_LIMIT_WINDOW_TIME,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_BURST_LIMIT_WINDOW_TIME,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,overload_kill_enable],
			default => ?LOG_OVERLOAD_KILL_ENABLE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_ENABLE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,overload_kill_qlen],
			default => ?LOG_OVERLOAD_KILL_QLEN,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_QLEN,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,overload_kill_mem_size],
			default => ?LOG_OVERLOAD_KILL_MEM_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_MEM_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,overload_kill_restart_after],
			default => ?LOG_OVERLOAD_KILL_RESTART_AFTER,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_OVERLOAD_KILL_RESTART_AFTER,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_restart_after]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,compress_on_rotate],
			default => ?LOG_COMPRESS_ON_ROTATE,
			type => boolean,
			runtime => true,
			short_description => ?LOG_DESC_COMPRESS_ON_ROTATE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,formatter,chars_limit],
			default => ?LOG_FORMATTER_CHARS_LIMIT,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_CHARS_LIMIT,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,formatter,max_size],
			default => ?LOG_FORMATTER_MAX_SIZE,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_MAX_SIZE,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,max_size]
			}
		},
		#{
			enabled => true,
			option_key => [logging,handlers,http,api,formatter,depth],
			default => ?LOG_FORMATTER_DEPTH,
			type => pos_integer,
			runtime => true,
			short_description => ?LOG_DESC_FORMATTER_DEPTH,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,depth]
			}
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Route and tune node logging behavior.">>.

%% @doc Dynamically apply a leaf value to a logger handler's config.
logger_set(_I, Value, _S, [HandlerId, formatter, Key]) ->
	case logger:get_handler_config(HandlerId) of
		{ok, #{formatter := {logger_formatter, Config}}} ->
			NewConfig = Config#{
				Key => Value
			},
			logger:update_handler_config(
				HandlerId,
				formatter,
				{logger_formatter, NewConfig}
			),
			{store, Value};
		_Else ->
			{store, Value}
	end;
logger_set(_K, Value, _S, [HandlerId, OptionKey, Key]) ->
	case logger:get_handler_config(HandlerId) of
		{ok, HandlerConfig} ->
			case maps:get(OptionKey, HandlerConfig, #{}) of
				C when is_map(C) ->
					logger:update_handler_config(
						HandlerId,
						OptionKey,
						C#{ Key => Value }
					),
					{store, Value};
				_ ->
					{store, Value}
			end;
		_Else ->
			{store, Value}
	end.
