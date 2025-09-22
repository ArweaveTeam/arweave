%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_sup).
-export([start_link/0]).
-export([init/1]).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(_Args) ->
	{ok, {supervisor(), children()}}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
supervisor() ->
	#{
		strategy => one_for_all
	 }.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
children() ->
	[
		#{
			id => arweave_config,
			start => {
				arweave_config,
				start_link,
				[]
			},
			type => worker
		},
	 	#{
		  	id => arweave_config_legacy,
			start => {
				arweave_config_legacy,
				start_link,
				[]
			},
			type => worker
		},
		#{
			id => arweave_config_spec,
			start => {
				arweave_config_spec,
				start_link,
				[arweave_config]
			},
			type => worker
		},
		#{
		  	id => arweave_config_store,
			start => {
				arweave_config_store,
				start_link,
				[]
			},
			type => worker
		},
		% check and configure parameters using
		% environment variable.
		#{
			id => arweave_config_environment,
			start => {
				arweave_config_environment,
				start_link,
				[]
			},
			type => worker,
			restart => transient
		}
		% check and configure parameters using
		% command line arguments.
		% #{
		% 	id => arweave_config_arguments,
		% 	start => {
		% 		arweave_config_arguments,
		% 		start_link,
		% 		[]
		% 	},
		% 	type => worker,
		% 	restart => transient
		% },
		% check and configure parameters using
		% configuration file.
		% #{
		%   	id => arweave_config_file,
		% 	start => {
		% 		arweave_config_file,
		% 		start_link,
		% 		[]
		% 	},
		% 	type => worker,
		% 	restart => transient
		% }
		% start configuration listener, if
		% defined
		% #{
		% 	id => arweave_config_listener,
		% 	start => {
		% 	  	arweave_config_listener,
		% 		start_link,
		% 		[]
		% 	},
		% 	type => worker
		% }
	].
