%%%===================================================================
%%% @doc Arweave Configuration Application.
%%%
%%% Arweave  Configuration Erlang  application  to  manage static  and
%%% dynamic arweave parameters during startup phase or runtime.
%%%
%%% == Design ==
%%%
%%% ```
%%%  _____________        ___________       ____________________
%%% |             |      |           |     |                    |
%%% | environment |      | arguments |     | configuration file |
%%% |_____________|      |___________|     |____________________|
%%%          |             |                 |
%%%          | +-----------+                 |
%%%          | | +---------------------------+
%%%          | | |
%%% ________ | | | ____________________________________[static]__
%%%          | | |                              [configuration]
%%%          | | |
%%%  _______ | | | _______________________________________________
%%% |        | | |                                                |
%%%  ________:_:_:__          _______       _________________     |
%%% |               |        |       |     |                 |    |
%%% | specification |------->| store |<----| arweave_config  |<-+ |
%%% |_______________|        |_______|     |_________________|  | |
%%%         : :  |         __________        _______________    | |
%%% |       | |  |        /          |      (               )   | |
%%% |       | |  +------->| callback |     ( arweave modules )--+ |
%%% |       | |           |_________/       (_______________)     |
%%% |       | |                                                   |
%%% |       | |                                                   |
%%% |       | |                                  [arweave_config] | 
%%% |       | |                                          [module] |
%%% |______ | |___________________________________________________|
%%%         | |
%%%  ______ | |_______________________________________[dynamic]__
%%%         | |                                 [configuration]
%%%         + +-----------+
%%%   ______|______   ____|_____
%%%  |             | |          |
%%%  | socket      | | rpc/eval |
%%%  |_____________| |__________|
%%%   ______|______   ____|_____
%%%  |             | |          |
%%%  | WUI/GUI/CLI | | CLI      |
%%%  |_____________| |__________|
%%%
%%% '''
%%%
%%% When Arweave  is started, `arweave_config' application  is started
%%% just  before (strong  requirement), to  prepare the  configuration
%%% parameters.
%%%
%%% Firstly,    `arweave_config'   will    load   all    configuration
%%% specifications in  memory, using `arweave_config_spec'  module and
%%% process. The specifications contain  the complete rule to describe
%%% a parameter, how to get or set it, but also the documentation.
%%%
%%% ```
%%% arweave_config_spec:start_link().
%%% '''
%%%
%%% Secondly, `arweave_config' will look  for all environment variable
%%% defined from  the specification.  When an environment  variable is
%%% found, the value is defined using the parameter key.
%%%
%%% ```
%%% arweave_config:load(environment).
%%%
%%% % or
%%%
%%% arweave_config_environment:load().
%%% '''
%%%
%%% Third,  `arweave_config' will  look for  the arguments  from the
%%% command line. The arguments will be parsed using the rule
%%% specified  from `arweave_config_spec'.  If  a  value match  and is
%%% correct, this one is set as parameter, even if the parameter    is
%%% already configured from environment variable.
%%%
%%% ```
%%% arweave_config:load(arguments).
%%% arweave_config:load({arguments, Arguments}).
%%%
%%% % or
%%%
%%% arweave_config_arguments:load().
%%% arweave_config_arguments:load(Arguments).
%%% '''
%%%
%%% Fourth,  `arweave_config' will  read  the  configuration file,  if
%%% provided,  and load  it. The  configuration file  parameters don't
%%% have priority over environment variables  and/or arguments if they
%%% have been previously defined.
%%%
%%% ```
%%% arweave_config:load(configuration).
%%% arweave_config:load({configuration, Path}).
%%%
%%% % or
%%%
%%% arweave_config_file:load().
%%% arweave_config_file:load(ConfigurationFile).
%%% '''
%%%
%%% Fifth,   `arweave_config'  should   now  be   ready  to   accept
%%% configuration events from outside world.
%%%
%%% ```
%%% % returns a parameter value.
%%% {ok, Value} = arweave_config:get(Key).
%%%
%%% % set a new value to a specific parameter.
%%% {ok, Value, Previous} = arweave_config:set(Key, Value).
%%%
%%% % show the whole configuration
%%% {ok, Parameters} = arweave_config:show().
%%%
%%% % returns the complete information (including value,
%%% % spec and documentation) from a key
%%% {ok, Info} = arweave_config:show(Key).
%%% '''
%%%
%%% Finally,   `arweave_config'  service   is  ready,   and  `arweave'
%%% application  can be  started  safely.  When `arweave'  terminates,
%%% `arweave_config' is stopped after.
%%%
%%% == Parameters ==
%%%
%%% The  parameters are  values stored  in `arweave_config_store'  and
%%% validated by `arweave_config_spec'. A parameter key is a list of
%%% term, usually atoms and binaries,  representing a path. An exemple
%%% with a global variable:
%%%
%%% ```
%%% [global, debug]
%%% [peers, <<"127.0.0.1:1234">>, enabled]
%%% [storage, 3, enabled]
%%% [storage, 3, unpacked]
%%% [storage, 3, 'replica.2.9']
%%% '''
%%%
%%% A parameter value is any kind of term stored using a path
%%% previously defined.
%%%
%%% == (todo) Wizard ==
%%%
%%% If `--wizard' argument or `AR_WIZARD=true' environment variables
%%% are set, a wizard is started to help user to configure `arweave'.
%%%
%%% == (todo) Specification Validation during Compilation ==
%%%
%%% The  specifications  for the  arguments  are  globally static  and
%%% defined one  time. In  this case,  those specifications  should be
%%% checked  during  compilation  time,  and  raise  an  error  before
%%% producing a compiled module.
%%%
%%% see: https://www.erlang.org/doc/apps/syntax_tools/merl.html
%%%
%%% see: https://github.com/2600hz/erlang-parse-trans
%%%
%%% == (todo) Auto-tuning ==
%%%
%%% If `--auto-tunning' argument  or `AR_AUTO_TUNING=true' environment
%%% variable are set,  `arweave_config' will try to  optimize and tune
%%% the configuration based on the system.
%%%
%%% Note: auto-tuning could also be done on the system side using
%%% `sysctl' functions for Unix/Linux OS.
%%%
%%% == (todo) Parameters Source Priority ==
%%%
%%% The first layer of configuration is from environment variable.
%%%
%%% The second layer of configuration  is from command line arguments,
%%% it will overwrite environment variable.
%%%
%%% The third  layer of configuration  is from configuration  file, it
%%% will overwrite configuration from both environment variable and
%%% command line.
%%%
%%% The last layer of configuration  is from runtime environment. When
%%% an user set dynamically a parameter during runtime, the
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
%%% To keep the old way  to configure Arweave, an environment variable
%%% can   be   used   to   switch   between   this   interface.   When
%%% `AR_CONFIG_LEGACY' is  set with  any kind of  value, configuration
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
%%%   "global": {
%%%     "debug": true,
%%%     "config_file": "",
%%%   },
%%%   "modules": {
%%%   },
%%%   "blocks": {},
%%%   "chunks": {},
%%%   "transactions": {},
%%%   "peers": {
%%%     "${peer}": {
%%%     }
%%%   },
%%%   "packing": {
%%%   },
%%%   "mining": {
%%%   },
%%%   "cooperative_mining": {
%%%   },
%%%   "storage": {
%%%     "3": {
%%%       "unpacked": {},
%%%       "replica.2.9": {},
%%%       "${type}":{}
%%%     },
%%%     "${index}": {}
%%%   }
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
%%% Only  available  during startup.  Those  variables  can alter  the
%%% configuration of an Arweave peer.
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
%%% == (todo) Command Line Interface ==
%%%
%%% A script can  communicate to an Erlang node  and execute functions
%%% if it has access to the cookie. This is not a very safe solution,
%%% but it  is easy and  quick to implement.  The only drawback  is to
%%% define a protocol  to communicate safely with the  node, and avoid
%%% command shell scripting mistakes (e.g. single/double quotes).
%%%
%%% Functions  can  be  executed  using  `erl_call'  command,  usually
%%% provided with any release. This command can execute a remote
%%% procedure call:
%%%
%%% ```
%%% # rpc call
%%% erl_call -r -c ${cookie} -a arweave_config_cli set "[${input}]"
%%% '''
%%%
%%% Or evaluate some code interpreted by the BEAM:
%%%
%%% ```
%%% # eval call
%%% cat | erl_call -r -c ${cookie} -e << EOF
%%% arweave_config_cli:set([${input}]).
%%% EOF
%%% '''
%%%
%%% The variable `${input}' should then  contain the arguments to pass
%%% to `arweave_config', but due to the problems listed above, it
%%% should  be  encoded using  base64.  The  string received  is  then
%%% sanitized and parsed.
%%%
%%% The returned value should also be encoded using base64.
%%%
%%% == (todo) Web User Interface ==
%%%
%%% `arweave_config' is able  to start a web interface  listening to a
%%% random port  (e.g. http://localhost:34945)  to give access  to the
%%% wizard or the configuration by an user.
%%%
%%% == (todo) Graphical User Interface ==
%%%
%%% `arweave_config' has  been designed  to support different  kind of
%%% interface, mainly using Erlang node to communicate. A graphical in
%%% interface any language can be designed and get access to the node.
%%%
%%% @end
%%%===================================================================
-module(arweave_config).
-behavior(application).
-export([
	start/0,
	stop/0,
	get/1,
	get/2,
	set/2,
	show/0,
	show/1, 
	spec/0,
	export/0,
	export/1,
	load/1
]).
-export([start/2, stop/1]).
-compile({no_auto_import,[get/1]}).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc helper function to started `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
start() ->
	application:ensure_all_started(?MODULE).

%%--------------------------------------------------------------------
%% @doc help function to stop `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
stop() ->
	application:stop(?MODULE).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
	{ok, Pid} = arweave_config_sup:start_link(),
	% ok = arweave_config_spec:init(?MODULE),
	{ok, Pid}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
stop(_Args) ->
	ok.

%%-------------------------------------------------------------------
%% @doc Returns the list of modules where parameters are defined.
%% @end
%%-------------------------------------------------------------------
spec() ->
	[
		arweave_config_global_data_directory,
		arweave_config_global_debug
		| arweave_config_parameters:init()
	].

%%--------------------------------------------------------------------
%% @doc Get a value from the configuration.
%%
%% == Examples ==
%%
%% ```
%% > get(<<"global.debug">>).
%% {ok, false}
%%
%% > get([global, debug]).
%% {ok, false}
%%
%% > get([test]).
%% {error, #{ reason => not_found }}.
%% '''
%%
%% @end
%%--------------------------------------------------------------------
get(Key) when is_atom(Key) ->
	% TODO: pattern to remove.
	% this pattern is ONLY for legacy purpose, it should be
	% removed after the full migration to the new arweave
	% configuration format.
	?LOG_DEBUG([
		{function, ?FUNCTION_NAME},
		{module, ?MODULE},
		{key, Key}
	]),
	arweave_config_legacy:get(Key);
get(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Parameter} ->
			arweave_config_store:get(Parameter);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc Get a value from the  configuration, if not defined, a default
%% value can be returned instead.
%%
%% == Examples ==
%%
%% ```
%% > get(<<"global.debug">>, true).
%% {ok, false}
%%
%% > get([global, debug], true).
%% {ok, false}
%%
%% > get([test] true).
%% {ok, true}
%% '''
%% @end
%%--------------------------------------------------------------------
get(Key, Default) ->
	case get(Key) of
		{ok, Value} ->
			{ok, Value};
		_Elsewise ->
			{ok, Default}
	end.

%%--------------------------------------------------------------------
%% @doc Set a configuration value using a key.
%%
%% == Examples==
%%
%% ```
%% > set(<<"global.debug">>, <<"true">>).
%% {ok, true}
%%
%% > set([global, debug]), true).
%% {ok, true}
%%
%% > set("global.debug", "true").
%% {ok, true}
%%
%% > set("global.debug", 1234).
%% {error, #{ reason => not_boolean }}
%% '''
%%
%% @end
%%--------------------------------------------------------------------
set(Key, Value) when is_atom(Key) ->
	% TODO: pattern to remove.
	% this pattern is ONLY for legacy purpose and should be
	% removed after the migration to the new arweave configuration
	% format.
	?LOG_DEBUG([
		{function, ?FUNCTION_NAME},
		{module, ?MODULE},
		{key, Key},
		{value, Value}
	]),
	arweave_config_legacy:set(Key, Value);
set(Key, Value) ->
	case arweave_config_parser:key(Key) of
		{ok, Parameter} ->
			arweave_config_spec:set(Parameter, Value);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc Returns the configuration keys, values with their
%% specifications.
%%
%% == Examples ==
%%
%% ```
%% {ok, #{ [global,debug] => {Value = true, Metadata = #{}} }} =
%%   arweave_config:show().
%% '''
%%
%% @end
%%--------------------------------------------------------------------
show() ->
	arweave_config_store:show().

%%--------------------------------------------------------------------
%% @doc Returns a key value and specification.
%%
%% == Examples ==
%%
%% ```
%% {ok, Value = true, Metadata = #{}} =
%%   show([global,debug]).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
show(Key) ->
	case arweave_config_parse:key(Key) of
		{ok, Parameter} ->
			arweave_config_store:get(Parameter);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc export the configuration as map.
%% @end
%%--------------------------------------------------------------------
export() ->
	arweave_config_store:export().

%%--------------------------------------------------------------------
%% @doc export the configuration in any other format (e.g. json).
%% @TODO: create the interface
%% @TODO: use the correct Module/Function.
%% @end
%%--------------------------------------------------------------------
export(legacy) ->
	% export the configuration as a tuple (or also called
	% #config{} record.
	todo;
export(json) ->
	% export the configuration in json format.
	Map = arweave_config_store:export(),
	export(json, Map);
export(yaml) ->
	% export the configuration in yaml format.
	Map = arweave_config_store:export(),
	export(yaml, Map);
export(inline) ->
	% export the configuration in "arweave inline format", where
	% the parameter is converted in key/value line by line.
	Map = arweave_config_store:export(),
	export(arweave_config_inliner, Map).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
export(Module, Map) ->
	Module:encode(Map).

%%--------------------------------------------------------------------
%% @doc Load manually different sources for the parameters. Interface
%% to many modules/functions.
%% @TODO: create the interface
%%
%% @see arweave_config_environment:load/0
%% @see arweave_config_arguments:load/0
%% @see arweave_config_arguments:load/1
%% @see arweave_config_file:load/0
%% @see arweave_config_file:load/1
%% @end
%%--------------------------------------------------------------------
-spec load(Sources) -> Return when
	Sources :: environment
		 | arguments | {arguments, Arguments}
		 | configuration | {configuration, Path},
	Arguments :: [string()],
	Path :: iolist(),
	Return :: ok | {error, Reason},
	Reason :: term().

load(environment) ->
	todo;
load(arguments) ->
	todo;
load({arguments, _Arguments}) ->
	todo;
load(configuration) ->
	todo;
load({configuration, _Path}) ->
	todo.
