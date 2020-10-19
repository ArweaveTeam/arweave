%% This Source Code Form is subject to the terms of the GNU General 
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed 
%% with this file, You can obtain one at 
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
	{ok, {{one_for_all, 5, 10}, [
		?CHILD(ar_node_worker, worker)
	]}}.
