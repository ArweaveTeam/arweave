%%%===================================================================
%%% @doc module in charge of the logging features.
%%% @end
%%%===================================================================
-module(ar_logger).
-export([init/1]).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%--------------------------------------------------------------------
%% @doc Uses #config{} record by default.
%% @end
%%--------------------------------------------------------------------
init(_Config) ->
	init_console(),
	init_default(),
	init_debug().

%%--------------------------------------------------------------------
%% @hidden
%% @doc Configure logging for console output.
%% @end
%%--------------------------------------------------------------------
init_console() ->
	Template = [time," [",level,"] ",mfa,":",line," ",msg,"\n"],
	LoggerFormatterConsole = #{
		legacy_header => false,
		single_line => true,
		chars_limit => 16256,
		max_size => 8128,
		depth => 256,
		template => Template
	},
	logger:set_handler_config(default, formatter, {logger_formatter, LoggerFormatterConsole}),
	logger:set_handler_config(default, level, error).

%%--------------------------------------------------------------------
%% @hidden
%% @doc Configure logging to the logfile.
%% @end
%%--------------------------------------------------------------------
init_default() ->
	Level = info,
	FileName = log_filename(#{ level => Level }),
	LogDir = arweave_config:get(log_dir),
	FilePath = filename:flatten(filename:join(LogDir, FileName)),
	LoggerConfigDisk = #{
		file => FilePath,
		type => file,
		max_no_files => 10,
		max_no_bytes => 51418800, % 10 x 5MB
		modes => [raw, append]
	},
	LoggerConfig = #{ config => LoggerConfigDisk, level => Level },
	Template = [time," [",level,"] ",mfa,":",line," ",msg,"\n"],
	LoggerFormatterDisk = #{
		chars_limit => 16256,
		max_size => 8128,
		depth => 256,
		legacy_header => false,
		single_line => true,
		template => Template
	},
	logger:add_handler(disk_log, logger_std_h, LoggerConfig),
	logger:set_handler_config(disk_log, formatter, {logger_formatter, LoggerFormatterDisk}).

%%--------------------------------------------------------------------
%% @hidden
%% @doc set debug logging feature
%% @end
%%--------------------------------------------------------------------
init_debug() ->
	case arweave_config:get(debug) of
		true ->
			Level = debug,
			FileName = log_filename(#{ level => Level }),
			LogDir = arweave_config:get(log_dir),
			FilePath = lists:flatten(filename:join([LogDir, FileName])),
			DebugLoggerConfigDisk = #{
				file => FilePath,
				type => file,
				max_no_files => 20,
				max_no_bytes => 51418800, % 10 x 5MB
				modes => [raw, append]
			},
			LoggerConfig = #{ config => DebugLoggerConfigDisk, level => Level },
			logger:add_handler(disk_debug_log, logger_std_h, LoggerConfig),
			logger:set_application_level(arweave, Level);
		_ ->
    			Level = info,
    			logger:set_application_level(arweave, Level)
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc returns a log filename.
%% @end
%%--------------------------------------------------------------------
log_filename(Opts) ->
    Prefix = maps:get(prefix, Opts, "arweave"),
    Level = maps:get(level, Opts, info),
    NodeName = erlang:node(),
    RawFileName = lists:join("-", ["arweave", NodeName, Level]),
    filename:flatten(RawFileName) ++ ".log".
