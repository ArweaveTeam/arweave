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
-export([init/0]).
-include_("arweave_config.hrl").

%%--------------------------------------------------------------------
%% @doc returns a list of map containing arweave parameters.
%% @end
%%--------------------------------------------------------------------
init() ->
	[
		#{
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
							logger:set_application_level(arweave_config, none),
							logger:set_application_level(arweave, none),
							legacy_set(K, V, S);
						{true, false} ->
							logger:set_application_level(arweave_config, debug),
							logger:set_application_level(arweave, debug),
							legacy_set(K, V, S)
					end;
				(K, V, S) ->
					logger:set_application_level(arweave_config, debug),
					logger:set_application_level(arweave, debug),
					legacy_set(K, V, S)
			end
		},

		%-----------------------------------------------------
		% arweave_config http api parameters
		%-----------------------------------------------------
		{
			parameter => [config,http,api,enabled],
			environment => <<"AR_CONFIG_HTTP_API_ENABLED">>,
			short_description => "enable arweave configuration http api interface",
			% @todo enable it by default after testing
			default => false,
			type => boolean,
			required => false
		},
		{
			parameter => [config,http,api,listen,port],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_PORT">>,
			short_description => "set arweave configuration http api interface port",
			default => 4891,
			type => tcp_port,
			required => false
		},
		{
			parameter => [config,http,api,listen,interface],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_INTERFACE">>,
			short_description => "set arweave configuration http api listen interface",
			type => ipv4,
			required => false,
			% can be an ip address or an unix socket path,
			% the configuration should be transparent
			% though and we should avoid using
			%   {local, socket_path}
			% the rule is probably to say if the value
			% start with / then this is an unix socket,
			% else this is an ip address or an hostname.
			default => <<"127.0.0.1">>
		}
		% @todo implement read, write and token parameters
		% {
		% 	parameter => [config,http,api,read],
		% 	environment => <<"AR_CONFIG_HTTP_API_READ">>,
		% 	short_description => "allow read (get method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% {
		% 	parameter => [config,http,api,write],
		% 	environment => <<"AR_CONFIG_HTTP_API_WRITE">>,
		% 	short_description => "allow write (post method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% {
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
	{ok, V}.

legacy_set(_K, V, #{ spec := #{ legacy := L }}) ->
	arweave_config_legacy:set(L, V),
	{store, V}.
