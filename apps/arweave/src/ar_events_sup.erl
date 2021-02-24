%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_events_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, I, Type), {I, {Mod, start_link, [I]}, permanent, 5000, Type, [Mod]}).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, {{one_for_one, 5, 10}, [
		% events: join/leave
		?CHILD(ar_events, network, worker),
		% events: joined/left/request (income requests)/response (for our requests)
		?CHILD(ar_events, peer, worker),
		% events: ban peer, set access restriction, etc...
		?CHILD(ar_events, access, worker)
	]}}.
