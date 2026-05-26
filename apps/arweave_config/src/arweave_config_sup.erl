%%% @doc Arweave Configuration Application Supervisor.
-module(arweave_config_sup).
-export([start_link/0]).
-export([init/1]).

%% @doc Start the arweave_config supervisor.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
	{ok, {supervisor(), children()}}.

supervisor() ->
	#{
		strategy => one_for_all
	 }.

children() ->
	[
		#{
			id => arweave_config,
			start => {
				arweave_config,
				start_link,
				[]
			}
		},
		#{
			id => arweave_config_store,
			start => {
				arweave_config_store,
				start_link,
				[]
			}
		},
		#{
			id => arweave_config_options_registry,
			start => {
				arweave_config_options_registry,
				start_link,
				[]
			}
		},
		#{
			id => arweave_config_signal_handler,
			start => {
				arweave_config_signal_handler,
				start_link,
				[]
			}
		}
	].
