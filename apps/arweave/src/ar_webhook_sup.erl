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

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Webhooks = [
        maps:without([enabled], Hook)
        || Hook <- arweave_config:get([webhooks]),
           maps:get(enabled, Hook, true) =:= true
    ],
    Children = lists:map(
        fun
            (Hook) when is_map(Hook) ->
                Handler = {ar_webhook, maps:get(url, Hook)},
                {Handler, {ar_webhook, start_link, [Hook]},
                    permanent, ?SHUTDOWN_TIMEOUT, worker, [ar_webhook]};
            (Hook) ->
                ?LOG_ERROR([{event, failed_to_parse_webhook_config},
                    {webhook_config, io_lib:format("~p", [Hook])}])
        end,
        Webhooks
    ),
    {ok, {{one_for_one, 5, 10}, Children}}.
