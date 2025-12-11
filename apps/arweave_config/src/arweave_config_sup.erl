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
%%% @doc Arweave Configuration Application Supervisor.
%%% @end
%%%===================================================================
-module(arweave_config_sup).
-export([start_link/0]).
-export([init/1]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_Args) ->
	{ok, {supervisor(), children()}}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
supervisor() ->
	#{
		strategy => one_for_all
	 }.

%%--------------------------------------------------------------------
%% @hidden
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
			id => arweave_config_environment,
			start => {
				arweave_config_environment,
				start_link,
				[]
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
		#{
			id => arweave_config_spec,
			start => {
				arweave_config_spec,
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
			id => arweave_config_signal_handler,
			start => {
				arweave_config_signal_handler,
				start_link,
				[]
			}
		}
	].
