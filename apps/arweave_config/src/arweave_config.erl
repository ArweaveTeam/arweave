%%%===================================================================
%%% @doc
%%%
%%% Arweave Configuration Erlang application to manage static and
%%% dynamic arweave parameters during startup phase or runtime.
%%%
%%% == (todo) Parameters Source Priority ==
%%%
%%% The first layer of configuration is from environment variable.
%%%
%%% The second layer of configuration is from command line arguments,
%%% it will overwrite environment variable.
%%%
%%% The third layer of configuration is from configuration file, it
%%% will overwrite configuration from both environment variable and
%%% command line.
%%%
%%% The last layer of configuration is from runtime environment.
%%% When an user set dynamically a parameter during runtime, the
%%% parameter is set ONLY in memory. A manual action is required
%%% to save the configuration locally (or export it).
%%%
%%% ```
%%% # check if the in memory configuration is the same than
%%% # the configuration from filesystem
%%% arweave config compare config.json
%%%
%%% # create a backup of the previous configuration file and
%%% # overwrite it with the configuration from memory.
%%% arweave config save --backup
%%%
%%% # export the configuration from memory to a file called
%%% # config.json.
%%% arweave config export config.json
%%% '''
%%%
%%% == (todo) Legacy Configuration ==
%%%
%%% To keep the old way to configure Arweave, an environment variable
%%% can be used to switch between this interface. When
%%% `AR_CONFIG_LEGACY' is set with any kind of value, configuration
%%% file and parameters will be interpreted as legacy configuration.
%%%
%%% ```
%%% export AR_CONFIG_LEGACY=true
%%% '''
%%%
%%% == (todo) Configuration File ==
%%%
%%% ```
%%% {
%%% }
%%% '''
%%%
%%% == (todo) Static Configuration ==
%%%
%%% ```
%%% arweave -c config.json -D data_dir
%%%
%%% arweave start -c config.json -D data_dir/
%%% arweave start --config-file=config.json --data-dir=data_dir/
%%%
%%% arweave config edit
%%% arweave config show
%%%
%%% arweave help
%%% arweave config help
%%% '''
%%%
%%% == (todo) Runtime Configuration ==
%%%
%%% ```
%%% arweave config global help
%%% arweave config global set debug true
%%% arweave config global set debug false
%%% '''
%%%
%%% === (todo) Peers Configuration Interface ===
%%%
%%% ```
%%% arweave config peers help
%%%
%%% # show the complete list of peers configured
%%% arweave config peers show
%%%
%%% # add a new peers (disabled by default)
%%% arweave config peers add 1.2.3.4
%%%
%%% # set some parameters to the new peers
%%% arweave config peers set 1.2.3.4 trusted
%%% arweave config peers set 1.2.3.4 vdf
%%% arweave config peers set 1.2.3.4 network.tcp.keepalive=true
%%%
%%% # enable the new peer
%%% arweave config peers enable 1.2.3.4
%%%
%%% # show the peer configuration
%%% arweave config peers show 1.2.3.4
%%%
%%% # disable peer
%%% arweave config peers disable 1.2.3.4
%%%
%%% # remove peer from the configuration
%%% arweave config peers remove 1.2.3.4
%%% '''
%%%
%%% === (todo) Storage Configuration Interface ===
%%%
%%% ```
%%% arweave config storage show
%%% arweave config storage create 3 unpacked
%%% arweave config storage sync 3 unpacked
%%% arweave config storage add 3 unpacked
%%% arweave config storage enable 3 unpacked
%%% arweave config storage disable 3 unpacked
%%% arweave config storage discard 3 unpacked
%%% '''
%%% 
%%% === (todo) Metrics Configuration Interface ===
%%%
%%% ```
%%% # enable metrics end-point
%%% arweave config metrics enable
%%%
%%% # show current metrics configuration
%%% arweave config metrics show
%%%
%%% # show weave_size metric configuration
%%% arweave config metrics show weave_size
%%%
%%% # get the value of weave_size metric
%%% arweave config metrics get weave_size
%%%
%%% # enable weave_size metric
%%% arweave config metrics enable weave_size
%%%
%%% # disable weave_size metric
%%% arweave config metrics disable weave_size
%%%
%%% # disable metrics end-point
%%% arweave config metrics disable
%%% '''
%%%
%%% == (todo) Environment Variables ==
%%%
%%% Only available during startup. Those variables can
%%% alter the configuration of an Arweave peer.
%%%
%%% ```
%%% export AR_CONFIG_LEGACY=true
%%% export AR_DEBUG=true
%%% '''
%%%
%%% == (todo) Embedded Documentation ==
%%%
%%% ```
%%% arweave help
%%% arweave ${command} help
%%% arweave ${command} ${subcommand} help
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config).
-behavior(application).
-export([start/2, stop/1]).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
	{ok, Pid} = arweave_config_sup:start_link(),
	ok = arweave_config:init(?MODULE),
	{ok, Pid}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
stop(_Args) ->
	ok.

%%-------------------------------------------------------------------
%%
%%-------------------------------------------------------------------
init() ->
	ok.

%%-------------------------------------------------------------------
%%
%%-------------------------------------------------------------------
spec() -> [
	arweave_config_global_data_directory,
	arweave_config_global_debug
].
