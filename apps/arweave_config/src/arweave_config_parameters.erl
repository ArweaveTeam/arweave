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
			long_argument => <<"--data-directory">>,
			handle_get => fun legacy_get/2,
			handle_set => fun legacy_set/3
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
				(K, V, S = #{ config := #{ debug := Old }}) ->
					case {V, Old} of
						{true, true} ->
							ignore;
						{false, false} ->
							ignore;
						{false, true} ->
							logger:set_application_level(arweave_config, info),
							logger:set_application_level(arweave, info),
							ar_logger:start_handler(arweave_debug),
							legacy_set(K, V, S);
						{true, false} ->
							logger:set_application_level(arweave_config, debug),
							logger:set_application_level(arweave, debug),
							ar_logger:stop_handler(arweave_debug),
							legacy_set(K, V, S)
					end;
				(K, V, S) ->
					logger:set_application_level(arweave_config, debug),
					logger:set_application_level(arweave, debug),
					ar_logger:start_handler(arweave_debug),
					legacy_set(K, V, S)
			end
		},

		%-----------------------------------------------------
		% arweave logging feature
		%-----------------------------------------------------
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% this parameter is not used yet.
			% TODO: create a special type for this
			% parameter, a templating like language will
			% be required to define this parameter from
			% the configuration or command line, something
			% like:
			%   "%time [%level] %msg\n"
			% will produce:
			%   [time," [",level,"] ",msg,"\n"]
			% where %xyz is a defined and valid atom.
			parameter_key => [logging,formatter,template],
			default => [time," [",level,"] ",mfa,":",line," ",msg,"\n"],
			% type => logging_template,
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
			runtime => false,
			handle_set => fun
				(_K, Path, _S) when is_list(Path) ->
					{store, Path};
				(_K, Path, _S) when is_binary(Path) ->
					{store, binary_to_list(Path)}
			end
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited).
			parameter_key => [logging,formatter,max_size],
			default => 8128,
			type => pos_integer,
			environmnet => true,
			runtime => true
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited)
			parameter_key => [logging,formatter,depth],
			default => 256,
			type => pos_integer,
			environment => true,
			runtime => true
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_formatter.html
			% TODO: this parameter can also be an atom
			% (unlimited)
			parameter_key => [logging,formatter,chars_limit],
			default => 16256,
			type => pos_integer,
			environment => true,
			runtime => true
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			parameter_key => [logging,max_no_files],
			default => 10,
			type => pos_integer,
			environment => true,
			runtime => true
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			% TODO: this parameter can also be an atom
			% (infinity)
			parameter_key => [logging,max_no_bytes],
			default => 51418800,
			type => pos_integer,
			environment => true,
			runtime => true
		},
		#{
			enabled => {false, wip},
			% see: https://www.erlang.org/doc/apps/kernel/logger_std_h.html
			parameter_key => [logging,compress_on_rotate],
			default => false,
			type => boolean,
			environment => true,
			runtime => true,
			% this is an example to show how to update all
			% running handlers during runtime.
			handle_set => fun
				(_K, V, _S) ->
					Handlers = ar_logger:started_handlers(),
					[
						begin
							{ok, C} = logger:get_handler_config(H),
							C2 = maps:get(config, C),
							C3 = C2#{ compress_on_rotate => V },
							logger:set_handler_config(H, config, C3)
						end
						||
						H <- Handlers
					]
			end
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,sync_mode_qlen]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,drop_mode_qlen]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,flush_qlen]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,burst_limit_enable]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,burst_limit_max_count]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,burst_limit_window_time]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,overload_kill_enable]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,overload_kill_qlen]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,overload_kill_mem_size]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,overload_kill_restart_after]
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,prefix],
			default => "arweave",
			type => string,
			environment => true,
			runtime => false
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,handlers,console],
			default => true,
			type => boolean,
			environment => true,
			runtime => false
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,handlers,info],
			default => true,
			type => boolean,
			environment => true,
			runtime => true,
			handle_set => fun
				(_K,true,_S) ->
					ar_logger:start_handler(arweave_info),
					{store, true};
				(_K,false,_S) ->
					ar_logger:stop_handler(arweave_info),
					{store, false}
			end
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,handlers,debug],
			default => false,
			type => boolean,
			environment => true,
			runtime => true,
			handle_set => fun
				(_K,true,_S) ->
					ar_logger:start_handler(arweave_debug),
					{store, true};
				(_K,false,_S) ->
					ar_logger:stop_handler(arweave_debug),
					{store, false}
			end
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,handlers,http,api],
			default => false,
			type => boolean,
			environment => true,
			runtime => true,
			handle_set => fun
				(_K,true,_S) ->
					ar_logger:start_handler(arweave_http_api),
					{store, true};
				(_K,false,_S) ->
					ar_logger:stop_handler(arweave_http_api),
					{store, false}
			end
		},
		#{
			enabled => {false, wip},
			parameter_key => [logging,handlers,http,api,compress_on_rotate],
			baseline => [logging,compress_on_rotate]
		},

		%-----------------------------------------------------
		% arweave_config http api parameters
		%-----------------------------------------------------
		#{
			parameter_key => [config,http,api,enabled],
			environment => <<"AR_CONFIG_HTTP_API_ENABLED">>,
			short_description => <<"enable arweave configuration http api interface">>,
			% @todo enable it by default after testing
			default => false,
			type => boolean,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,port],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_PORT">>,
			short_description => "set arweave configuration http api interface port",
			default => 4891,
			type => tcp_port,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,address],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_ADDRESS">>,
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

legacy_get(_K, #{ spec := #{ legacy := L }}) ->
	V = arweave_config_legacy:get(L),
	{ok, V};
legacy_get(_K, _) ->
	{error, not_found}.

legacy_set(_K, V, #{ spec := #{ legacy := L }}) ->
	arweave_config_legacy:set(L, V),
	{store, V};
legacy_set(_K, V, _) ->
	{store, V}.
