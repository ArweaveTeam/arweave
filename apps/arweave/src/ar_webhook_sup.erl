%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_webhook_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("arweave/include/ar_config.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	Children = lists:map(
		fun
			(Hook) when is_record(Hook, config_webhook) ->
				Handler = {ar_webhook, Hook#config_webhook.url},
				{Handler, {ar_webhook, start_link, [Hook]},
					permanent, 5000, worker, [ar_webhook]};
			(Hook) ->
				?LOG_ERROR([{event, failed_to_parse_webhook_config},
					{webhook_config, io_lib:format("~p", [Hook])}])
		end,
		Config#config.webhooks
	),
	{ok, {{one_for_one, 5, 10}, Children}}.
