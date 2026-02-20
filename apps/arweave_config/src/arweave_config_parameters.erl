%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration Parameters.
%%%
%%% == TODO ==
%%%
%%% @todo create an `include' parameter to include files from
%%% other places.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_parameters).
-compile(warnings_as_errors).
-export([init/0]).
-include_("arweave_config.hrl").

%%--------------------------------------------------------------------
%% @doc returns a list of map containing arweave parameters.
%% @end
%%--------------------------------------------------------------------
init() ->
	[
		% set configuration file. The configuration parameter
		% is a list of binary, stored in arweave_config_file
		% process.
		#{
			enabled => true,
			parameter_key => [configuration],
			default => [],
			type => path,
			runtime => true,
			deprecated => false,
			required => false,
			environment => <<"AR_CONFIGURATION">>,
			short_argument => $c,
			long_argument => <<"--configuration">>,
			handle_set => fun
				(_, V, _ ,_) ->
					{ok, _} = arweave_config_file:add(V),
					P = arweave_config_file:get_paths(),
					{store, P}
			end,
			handle_get => fun
				(_, _) ->
					{ok, arweave_config_file:get_paths()}
			end
		},

		% set data directory
		#{
			enabled => true,
			parameter_key => [data,directory],
			default => "./data",
			runtime => false,
			type => path,
			deprecated => false,
			legacy => data_dir,
			required => true,
			short_description => "",
			long_description => "",
			environment => <<"AR_DATA_DIRECTORY">>,
			short_argument => $D,
			long_argument => <<"--data.directory">>,
			handle_get => fun legacy_get/2,
			handle_set => fun legacy_set/4
		},
		#{
			parameter_key => [start,from,state],
			default => not_set,
			runtime => false,
			type => path,
			deprecated => false,
			legacy => start_from_state,
			required => false,
			short_description => "",
			long_description => "",
			environment => <<"AR_START_FROM_STATE">>,
			long_argument => <<"--start-from-state">>,
			handle_get => fun legacy_get/2,
			handle_set => fun legacy_set/4
		},
	 	#{
			enabled => true,
			parameter_key => [debug],
			default => false,
			runtime => true,
			type => boolean,
			deprecated => false,
			legacy => debug,
			required => false,
			short_description => "",
			long_description => "",
			environment => <<"AR_DEBUG">>,
			short_argument => $d,
			long_argument => <<"--debug">>,
			handle_get => fun legacy_get/2,
			handle_set => fun
				(K, V, S = #{ config := #{ debug := Old }}, _) ->
					case {V, Old} of
						{true, true} ->
							ignore;
						{false, false} ->
							ignore;
						{false, true} ->
							logger:set_application_level(arweave_config, info),
							logger:set_application_level(arweave, info),
							ar_logger:stop_handler(arweave_debug),
							legacy_set(K, V, S, []);
						{true, false} ->
							logger:set_application_level(arweave_config, debug),
							logger:set_application_level(arweave, debug),
							ar_logger:start_handler(arweave_debug),
							legacy_set(K, V, S, [])
					end;
				(K, V, S, _) ->
					logger:set_application_level(arweave_config, debug),
					logger:set_application_level(arweave, debug),
					ar_logger:start_handler(arweave_debug),
					legacy_set(K, V, S, [])
			end
		},

		%-----------------------------------------------------
		% arweave logging feature
		%-----------------------------------------------------
		#{
			enabled => true,
			% parse a string and convert it to valid
			% logger template.
			parameter_key => [logging,formatter,template],
			default => [time," [",level,"] ",mfa,":",line," ",msg,"\n"],
			type => logging_template,
			environment => false,
			runtime => false
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger.html
			% config.path must be a string (list of
			% integer). By default, type path will convert
			% its input in binary. So, one can convert the
			% value stored using handle_get, or converting
			% the value when using handle_set.
			parameter_key => [logging,path],
			default => "./logs",
			type => path,
			environment => true,
			long_argument => true,
			runtime => false,
			handle_set => fun
				(_K, Path, _S, _) when is_list(Path) ->
					{store, Path};
				(_K, Path, _S, _) when is_binary(Path) ->
					{store, binary_to_list(Path)}
			end
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited).
			parameter_key => [logging,formatter,max_size],
			default => 8128,
			type => pos_integer,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,max_size]
			}
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited)
			parameter_key => [logging,formatter,depth],
			default => 256,
			type => pos_integer,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,depth]
			}
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited)
			parameter_key => [logging,formatter,chars_limit],
			default => 16256,
			type => pos_integer,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			parameter_key => [logging,max_no_files],
			default => 20,
			type => pos_integer,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,max_no_files]
			}
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			% TODO: this parameter can also be an atom
			% (infinity)
			parameter_key => [logging,max_no_bytes],
			default => 51418800,
			type => pos_integer,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			parameter_key => [logging,compress_on_rotate],
			default => false,
			type => boolean,
			environment => true,
			long_argument => true,
			runtime => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,sync_mode_qlen],
			runtime => false,
			type => pos_integer,
			default => 10,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,drop_mode_qlen],
			runtime => true,
			type => pos_integer,
			default => 200,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,flush_qlen],
			runtime => true,
			type => pos_integer,
			default => 1000,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,burst_limit_enable],
			runtime => true,
			type => boolean,
			default => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,burst_limit_max_count],
			runtime => true,
			type => pos_integer,
			default => 500,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,burst_limit_window_time],
			runtime => true,
			type => pos_integer,
			default => 1000,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,overload_kill_enable],
			runtime => true,
			type => boolean,
			default => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,overload_kill_qlen],
			runtime => true,
			type => pos_integer,
			default => 20_000,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,overload_kill_mem_size],
			runtime => true,
			type => pos_integer,
			default => 3_000_000,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,overload_kill_restart_after],
			runtime => true,
			type => pos_integer,
			default => 5000,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_info,config,overload_kill_restart_after]
			}
		},

		% debug logs
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug],
			default => false,
			type => boolean,
			environment => true,
			long_argument => true,
			runtime => true,
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
			parameter_key => [logging,handlers,debug,max_no_files],
			inherit => [logging,max_no_files],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,max_no_files]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,max_no_bytes],
			inherit => [logging,max_no_bytes],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,sync_mode_qlen],
			inherit => [logging,sync_mode_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,drop_mode_qlen],
			inherit => [logging,drop_mode_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,flush_qlen],
			inherit => [logging,flush_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,burst_limit_enable],
			inherit => [logging,burst_limit_enable],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,burst_limit_max_count],
			inherit => [logging,burst_limit_max_count],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,burst_limit_window_time],
			inherit => [logging,burst_limit_window_time],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,overload_kill_enable],
			inherit => [logging,overload_kill_enable],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,overload_kill_qlen],
			inherit => [logging,overload_kill_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,overload_kill_mem_size],
			inherit => [logging,overload_kill_mem_size],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,overload_kill_restart_after],
			inherit => [logging,overload_kill_restart_after],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,config,overload_kill_restart_after]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,formatter,chars_limit],
			inherit => [logging,formatter,chars_limit],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,formatter,depth],
			inherit => [logging,formatter,depth],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,depth]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,formatter,max_size],
			inherit => [logging,formatter,max_size],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,max_size]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,debug,formatter,template],
			inherit => [logging,formatter,template],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_debug,formatter,template]
			}
		},

		% http api logs
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api],
			default => false,
			type => boolean,
			environment => true,
			long_argument => true,
			runtime => true,
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
			parameter_key => [logging,handlers,http,api,max_no_files],
			inherit => [logging,max_no_files],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,max_no_files]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,max_no_bytes],
			inherit => [logging,max_no_bytes],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,max_no_bytes]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,sync_mode_qlen],
			inherit => [logging,sync_mode_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,sync_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,drop_mode_qlen],
			inherit => [logging,drop_mode_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,drop_mode_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,flush_qlen],
			inherit => [logging,flush_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,flush_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,burst_limit_enable],
			inherit => [logging,burst_limit_enable],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,burst_limit_max_count],
			inherit => [logging,burst_limit_max_count],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_max_count]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,burst_limit_window_time],
			inherit => [logging,burst_limit_window_time],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,burst_limit_window_time]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,overload_kill_enable],
			inherit => [logging,overload_kill_enable],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_enable]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,overload_kill_qlen],
			inherit => [logging,overload_kill_qlen],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_qlen]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,overload_kill_mem_size],
			inherit => [logging,overload_kill_mem_size],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_mem_size]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,overload_kill_restart_after],
			inherit => [logging,overload_kill_restart_after],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,overload_kill_restart_after]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,compress_on_rotate],
			inherit => {[logging,compress_on_rotate], [type, default]},
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,config,compress_on_rotate]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,formatter,chars_limit],
			inherit => [logging,formatter,chars_limit],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,chars_limit]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,formatter,max_size],
			inherit => [logging,formatter,max_size],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,max_size]
			}
		},
		#{
			enabled => true,
			parameter_key => [logging,handlers,http,api,formatter,depth],
			inherit => [logging,formatter,depth],
			runtime => true,
			environment => true,
			long_argument => true,
			handle_set => {
				fun logger_set/4,
				[arweave_http_api,formatter,depth]
			}
		},

		%-----------------------------------------------------
		% arweave_config http api parameters
		%-----------------------------------------------------
		#{
			parameter_key => [config,http,api,enabled],
			environment => true,
			long_argument => true,
			short_description => <<"enable arweave configuration http api interface">>,
			% @todo enable it by default after testing
			default => false,
			type => boolean,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,port],
			environment => true,
			long_argument => true,
			short_description => "set arweave configuration http api interface port",
			default => 4891,
			type => tcp_port,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,address],
			environment => true,
			long_argument => true,
			short_description => "set arweave configuration http api listen address",
			type => [ipv4, file],
			required => false,
			% can be an ip address or an unix socket path,
			% the configuration should be transparent
			% though and we should avoid using
			%   {local, socket_path}
			% the rule is probably to say if the value
			% start with / then this is an unix socket,
			% else this is an ip address or an hostname.
			default => <<"127.0.0.1">>,
			runtime => false
		}
		% @todo implement read, write and token parameters
		% #{
		% 	parameter => [config,http,api,read],
		% 	environment => <<"AR_CONFIG_HTTP_API_READ">>,
		% 	short_description => "allow read (get method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% #{
		% 	parameter => [config,http,api,write],
		% 	environment => <<"AR_CONFIG_HTTP_API_WRITE">>,
		% 	short_description => "allow write (post method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% #{
		% 	parameter => [config,http,api,token],
		% 	environment => <<"AR_CONFIG_HTTP_API_TOKEN">>,
		% 	short_description => "set an access token for arweave configuration http api interface",
		% 	type => string,
		% 	required => false,
		% 	default => <<>>
		% }
	].

%%--------------------------------------------------------------------
%% @hidden
%% @doc function helper to deal with legacy configuration.
%% @end
%%--------------------------------------------------------------------
legacy_get(_K, #{ spec := #{ legacy := L }}) ->
	V = arweave_config_legacy:get(L),
	{ok, V};
legacy_get(_K, _) ->
	{error, not_found}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc function helper to deal with legacy configuration.
%% @end
%%--------------------------------------------------------------------
legacy_set(_K, V, #{ spec := #{ legacy := L }},_) ->
	arweave_config_legacy:set(L, V),
	{store, V};
legacy_set(_K, V, _,_) ->
	{store, V}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc helper function to dynamically set logging handlers.
%% @end
%%--------------------------------------------------------------------
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
logger_set(_K, Value, _S, [HandlerId, ConfigKey, Key]) ->
	case logger:get_handler_config(HandlerId) of
		{ok, HandlerConfig} ->
			C = maps:get(ConfigKey, HandlerConfig),
			logger:update_handler_config(
				HandlerId,
				ConfigKey,
				C#{ Key => Value }
			),
			{store, Value};
		_Else ->
			{store, Value}
	end.
