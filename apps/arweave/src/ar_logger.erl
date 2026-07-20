%%% @doc Arweave Logging Interface.
%%%
%%% This module is in charge of starting, stopping, enabling,
%%% disabling logging Arweave handlers.
%%%
%%% == Logger Primary Configuration ==
%%%
%%% Primary logger configuration is defined in `config/sys.config',
%%% with the help of `logger_level' key.
%%%
%%% see: https://www.erlang.org/doc/apps/kernel/logger_chapter
%%%
%%% == Logger Default Configuration ==
%%%
%%% The default configuration is used to log to the console, and it
%%% should be not modified by default. To avoid modify this, this
%%% value is defined outside of this module, in `config/sys.config'
%%% via the help of `logger' key.  Here the configuration:
%%%
%%% ```
%%% [{handler, default, logger_std_h, #{
%%%     level => warning,
%%%     formatter => {
%%%       logger_formatter, #{
%%%         legacy_header => false,
%%%         single_line => true,
%%%         chars_limit => 16256,
%%%         max_size => 8128,
%%%         depth => 256,
%%%         template => [time," [",level,"] ",mfa,":",line," ",msg,"\n"]
%%%       }
%%%     }
%%%   }
%%% }].
%%% '''
%%%
%%% see: https://www.erlang.org/doc/apps/kernel/logger_chapter
%%% @end
%%% @see logger
%%% @see logger_handler
%%% @TODO integrate with arweave_config.
%%% @TODO create domain for different part of the code, but all
%%%       calls to logger inside arweave should be in the domain
%%%       [arweave].
%%% @TODO ensure primary logger configuration is set with the right
%%%       values (level => all).
%%%===================================================================
-module(ar_logger).
-compile(warnings_as_errors).
-export([
         init/0,
         is_started/1,
         handlers/0,
         start_handlers/0,
         start_handler/1,
         started_handlers/0,
         stop_handlers/0,
         stop_handler/1,
         gen_log/3
        ]).
-include_lib("arweave/include/ar.hrl").

%%--------------------------------------------------------------------
%% @doc Boot the logger handlers. Reads `[debug]' from the options registry
%% to decide whether to start the dedicated debug handler.
%% @end
%%--------------------------------------------------------------------
init() ->
    start_handler(default),
    start_handler(arweave_info),
    Debug = arweave_config:get([debug]),
    case Debug of
        true  -> start_handler(arweave_debug);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% @doc wrapper around `logger:get_handler_config/1'.
%% @see logger:get_handle_config/1
%% @end
%%--------------------------------------------------------------------
is_started(Handler) ->
    case logger:get_handler_config(Handler) of
        {ok, _} -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @doc defined loggers.
%% @end
%%--------------------------------------------------------------------
handlers() -> #{
                                                % log every info message.
                                                % This handler can be configured on demand, all message
                                                % greater or equal than info are being logged.
                arweave_info => #{
                                  level => info,
                                  config => #{
                                              type => file,
                                              file => logfile_path(#{
                                                                     prefix => "arweave",
                                                                     level => info
                                                                    }),
                                              compress_on_rotate => arweave_config:get([logging,compress_on_rotate]),
                                              max_no_files => arweave_config:get([logging,sync_mode_qlen]),
                                              max_no_bytes => arweave_config:get([logging,max_no_bytes]),
                                              modes => [raw, append],
                                              sync_mode_qlen => arweave_config:get([logging,sync_mode_qlen]),
                                              drop_mode_qlen => arweave_config:get([logging,drop_mode_qlen]),
                                              flush_qlen => arweave_config:get([logging,flush_qlen]),
                                              burst_limit_enable => arweave_config:get([logging,burst_limit_enable]),
                                              burst_limit_max_count => arweave_config:get([logging,burst_limit_max_count]),
                                              burst_limit_window_time => arweave_config:get([logging,burst_limit_window_time]),
                                              overload_kill_enable => arweave_config:get([logging,overload_kill_enable]),
                                              overload_kill_qlen => arweave_config:get([logging,overload_kill_qlen]),
                                              overload_kill_mem_size => arweave_config:get([logging,overload_kill_mem_size]),
                                              overload_kill_restart_after => arweave_config:get([logging,overload_kill_restart_after])
                                             },
                                  formatter => {
                                                logger_formatter, #{
                                                                    chars_limit => arweave_config:get([logging,formatter,chars_limit]),
                                                                    depth => arweave_config:get([logging,formatter,depth]),
                                                                    legacy_header => false,
                                                                    max_size => arweave_config:get([logging,formatter,max_size]),
                                                                    single_line => true,
                                                                    template => arweave_config:get([logging,formatter,template]),
                                                                    time_offset => "Z"
                                                                   }
                                               },
                                  filter_default => log,
                                  filters => [
                                              {n_wildcard, {fun logger_filters:level/2, {stop, lt, info}}},
                                              {n_http, {fun logger_filters:domain/2, {stop, sub, [arweave,http]}}}
                                             ]
                                 },

                                                % log every debug message.
                                                % Only debug messages are being logged.
                arweave_debug => #{
                                   level => debug,
                                   config => #{
                                               type => file,
                                               file => logfile_path(#{
                                                                      prefix => "arweave",
                                                                      level => debug
                                                                     }),
                                               compress_on_rotate => arweave_config:get([logging,handlers,debug,compress_on_rotate]),
                                               max_no_files => arweave_config:get([logging,handlers,debug,max_no_files]),
                                               max_no_bytes => arweave_config:get([logging,handlers,debug,max_no_bytes]),
                                               modes => [raw, append],
                                               sync_mode_qlen => arweave_config:get([logging,handlers,debug,sync_mode_qlen]),
                                               drop_mode_qlen => arweave_config:get([logging,handlers,debug,drop_mode_qlen]),
                                               flush_qlen => arweave_config:get([logging,handlers,debug,flush_qlen]),
                                               burst_limit_enable => arweave_config:get([logging,handlers,debug,burst_limit_enable]),
                                               burst_limit_max_count => arweave_config:get([logging,handlers,debug,burst_limit_max_count]),
                                               burst_limit_window_time => arweave_config:get([logging,handlers,debug,burst_limit_window_time]),
                                               overload_kill_enable => arweave_config:get([logging,handlers,debug,overload_kill_enable]),
                                               overload_kill_qlen => arweave_config:get([logging,handlers,debug,overload_kill_qlen]),
                                               overload_kill_mem_size => arweave_config:get([logging,handlers,debug,overload_kill_mem_size]),
                                               overload_kill_restart_after => arweave_config:get([logging,handlers,debug,overload_kill_restart_after])
                                              },
                                   formatter => {
                                                 logger_formatter, #{
                                                                     chars_limit => arweave_config:get([logging,handlers,debug,formatter,chars_limit]),
                                                                     depth => arweave_config:get([logging,handlers,debug,formatter,depth]),
                                                                     legacy_header => false,
                                                                     max_size => arweave_config:get([logging,handlers,debug,formatter,max_size]),
                                                                     single_line => true,
                                                                     template => arweave_config:get([logging,handlers,debug,formatter,template]),
                                                                     time_offset => "Z"
                                                                    }
                                                },
                                   filter_default => log,
                                   filters => [
                                               {n_wildcard, {fun logger_filters:level/2, {stop, lt, debug}}},
                                               {n_http, {fun logger_filters:domain/2, {stop, sub, [arweave,http]}}}
                                              ]
                                  },

                                                % this handler will log only log message containing the domain
                                                % [arweave,http,api] in info level.
                arweave_http_api => #{
                                      level => info,
                                      config => #{
                                                  type => file,
                                                  file => logfile_path(#{
                                                                         prefix => "arweave-http-api",
                                                                         level => debug
                                                                        }),
                                                  compress_on_rotate => arweave_config:get([logging,handlers,http,api,compress_on_rotate]),
                                                  max_no_files => arweave_config:get([logging,handlers,http,api,max_no_files]),
                                                  max_no_bytes => arweave_config:get([logging,handlers,http,api,max_no_bytes]),
                                                  modes => [raw, append],
                                                  sync_mode_qlen => arweave_config:get([logging,handlers,http,api,sync_mode_qlen]),
                                                  drop_mode_qlen => arweave_config:get([logging,handlers,http,api,drop_mode_qlen]),
                                                  flush_qlen => arweave_config:get([logging,handlers,http,api,flush_qlen]),
                                                  burst_limit_enable => arweave_config:get([logging,handlers,http,api,burst_limit_enable]),
                                                  burst_limit_max_count => arweave_config:get([logging,handlers,http,api,burst_limit_max_count]),
                                                  burst_limit_window_time => arweave_config:get([logging,handlers,http,api,burst_limit_window_time]),
                                                  overload_kill_enable => arweave_config:get([logging,handlers,http,api,overload_kill_enable]),
                                                  overload_kill_qlen => arweave_config:get([logging,handlers,http,api,overload_kill_qlen]),
                                                  overload_kill_mem_size => arweave_config:get([logging,handlers,http,api,overload_kill_mem_size]),
                                                  overload_kill_restart_after => arweave_config:get([logging,handlers,http,api,overload_kill_restart_after])
                                                 },
                                      formatter => {
                                                    logger_formatter, #{
                                                                        legacy_header => false,
                                                                        single_line => true,
                                                                        chars_limit => arweave_config:get([logging,handlers,http,api,formatter,chars_limit]),
                                                                        max_size => arweave_config:get([logging,handlers,http,api,formatter,max_size]),
                                                                        depth => arweave_config:get([logging,handlers,http,api,formatter,depth]),
                                                                        template => [
                                                                                     time, " ",
                                                                                     "ip=", peer_ip, " ",
                                                                                     "port=", peer_port, " ",
                                                                                     "version=", version, " ",
                                                                                     "method=", method, " ",
                                                                                     "code=", code, " ",
                                                                                     "path=", path, " ",
                                                                                     "body_length=", body_length, " ",
                                                                                     "duration=", duration, " ",
                                                                                     "msg=", msg, "\n"
                                                                                    ],
                                                                        time_offset => "Z"
                                                                       }
                                                   },
                                      filter_default => stop,
                                      filters => [
                                                  {info, {fun logger_filters:level/2, {stop, lt, info}}},
                                                  {http, {fun logger_filters:domain/2, {log, sub, [arweave,http,api]}}}
                                                 ]
                                     }
               }.

%%--------------------------------------------------------------------
%% @doc start all defined loggers.
%% @end
%%--------------------------------------------------------------------
start_handlers() ->
    Handlers = maps:keys(handlers()),
    [ start_handler(Handler) || Handler <- Handlers ].

%%--------------------------------------------------------------------
%% @doc start one defined logger.
%% @end
%%--------------------------------------------------------------------
start_handler(Handler) ->
    case maps:get(Handler, handlers(), undefined) of
        undefined ->
            {error, not_found};
        Config ->
            start_handler(Handler, Config)
    end.

start_handler(Handler, Config) ->
    case is_started(Handler) of
        true ->
            ok;
        false ->
            logger:add_handler(Handler, logger_std_h, Config)
    end.

%%--------------------------------------------------------------------
%% @doc stop all loggers set.
%% @end
%%--------------------------------------------------------------------
stop_handlers() ->
    Handlers = maps:keys(handlers()),
    [ stop_handler(Handler) || Handler <- Handlers ].

%%--------------------------------------------------------------------
%% @doc stop logger.
%% @end
%%--------------------------------------------------------------------
stop_handler(Handler) -> logger:remove_handler(Handler).

%%--------------------------------------------------------------------
%% @hidden
%% @doc list started handlers.
%% @end
%%--------------------------------------------------------------------
started_handlers() ->
    HandlersIds = maps:keys(handlers()),
    #{ handlers := HandlersStarted } = logger:get_config(),
    [ Id ||
        #{ id := Id } <- HandlersStarted,
        Id2 <- HandlersIds,
        Id =:= Id2
    ].

%%--------------------------------------------------------------------
%% @hidden
%% @doc function only used to write to logs during test.
%% @end
%%--------------------------------------------------------------------
gen_log(Format, FormatMsg, Meta) ->
    ?LOG_EMERGENCY(Format, FormatMsg, Meta),
    ?LOG_ALERT(Format, FormatMsg, Meta),
    ?LOG_CRITICAL(Format, FormatMsg, Meta),
    ?LOG_ERROR(Format, FormatMsg, Meta),
    ?LOG_WARNING(Format, FormatMsg, Meta),
    ?LOG_NOTICE(Format, FormatMsg, Meta),
    ?LOG_INFO(Format, FormatMsg, Meta),
    ?LOG_DEBUG(Format, FormatMsg, Meta).

%%--------------------------------------------------------------------
%% @hidden
%% @doc returns a log filename.
%% @end
%%--------------------------------------------------------------------
logfile_path(Opts) ->
                                                % TODO: if arweave_config is not set, even with a default
                                                % value set, this part of the code crashes.
    LogDir = arweave_config:get([logging, path]),
    Prefix = maps:get(prefix, Opts),
    Level = maps:get(level, Opts),
    NodeName = erlang:node(),
    RawFilename = lists:join("-", [Prefix, NodeName, Level]),
    Filename = filename:flatten(RawFilename) ++ ".log",
    filename:flatten(filename:join(LogDir, Filename)).
