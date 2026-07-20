%%% @doc Specs for the `config` option group. Options for
%%% controlling access to the node's configuration service.
-module(arweave_config_options_config).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

%% The `[config, http, ...]` group is not production-ready. Operators
%% can set these options, but `arweave_config_http_server:start_as_child/0'
%% is gated under `-ifdef(AR_TEST)' and refuses to launch outside the
%% test profile, so setting these has no effect on a release build.
%% Remove that guard (and the warnings here) when the feature ships.
specs() ->
    [
        #{
            option_key => [config,http,enabled],
            short_description =>
                <<"Enable the Arweave configuration HTTP API interface.">>,
            long_description =>
                <<"NOT YET IMPLEMENTED. Setting this has no effect on a "
                  "release build; the HTTP config server is gated and "
                  "refuses to start outside the test profile.">>,
            default => false,
            type => boolean,
            required => false,
            runtime => false
        },
        #{
            option_key => [config,http,listen,port],
            short_description =>
                <<"Set the Arweave configuration HTTP API port.">>,
            long_description =>
                <<"NOT YET IMPLEMENTED. See [config, http, enabled].">>,
            default => 4891,
            type => tcp_port,
            required => false,
            runtime => false
        },
        #{
            option_key => [config,http,listen,address],
            short_description =>
                <<"Set the Arweave configuration HTTP API listen address.">>,
            long_description =>
                <<"NOT YET IMPLEMENTED. See [config, http, enabled]. "
                  "A value starting with `/` is interpreted as a Unix "
                  "socket path, anything else as an IP address or "
                  "hostname.">>,
            type => [ipv4, file],
            required => false,
            default => <<"127.0.0.1">>,
            runtime => false
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage configuration API exposure and listener behavior.">>.
