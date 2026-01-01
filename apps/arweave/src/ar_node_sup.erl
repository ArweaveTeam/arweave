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

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_sup.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
	{ok, {supervisor_spec(), children_spec()}}.

supervisor_spec() ->
	#{ strategy => one_for_all
	 , intensity => 5
	 , period => 10
	 }.

%%--------------------------------------------------------------------
%% the order is important. the first process to be started is
%% ar_node_worker, then other processes in order. The shutdown
%% is in reverse, the last process to be stopped is ar_node_worker.
%%--------------------------------------------------------------------
children_spec() ->
	lists:flatten([
		ar_node_worker_spec(),
		ar_semaphores_spec(),
		ar_blacklist_middleware_spec(),
		ar_http_iface_server_spec()
	]).

%%--------------------------------------------------------------------
%% ar_node_worker is the main process, must be started before others,
%% and should be stopped at last. This process should not be restarted.
%%--------------------------------------------------------------------
ar_node_worker_spec() ->
	#{ id => ar_node_worker
	 , start => {ar_node_worker, start_link, []}
	 , type => worker
	 , shutdown => ?SHUTDOWN_TIMEOUT
	 , restart => temporary
	 }.

%%--------------------------------------------------------------------
%% ar_http_iface_server process is a frontend to cowboy:start*/3,
%% and will return a worker. This worker is protecting the
%% cowboy listener (stored in its state). The timeout should be
%% greater or equal to the TCP_MAX_CONNECTION to avoid killing the
%% child too early during the shutdown procedure.
%%--------------------------------------------------------------------
ar_http_iface_server_spec() ->
	#{ id => ar_http_iface_server
	 , start => {ar_http_iface_server, start_link, []}
	 , type => worker
	 , shutdown => ?SHUTDOWN_TCP_CONNECTION_TIMEOUT*2*1000
	 }.

%%--------------------------------------------------------------------
%% ar_blacklist_middle process is transient, it will configure
%% a timer and then return. In case of error, it should be
%% restarted.
%%--------------------------------------------------------------------
ar_blacklist_middleware_spec() ->
	#{ id => ar_blacklist_middleware
	 , start => {ar_blacklist_middleware, start_link, []}
	 , type => worker
	 , restart => transient
	 , shutdown => ?SHUTDOWN_TIMEOUT
	 }.

%%--------------------------------------------------------------------
%% ar_semaphores are processes started based on arweave
%% configuration.
%%--------------------------------------------------------------------
ar_semaphores_spec() ->
	{ok, Config} = arweave_config:get_env(),
	Semaphores = Config#config.semaphores,
	[ ar_semaphore_spec(Name, N) || {Name, N} <- maps:to_list(Semaphores) ].

ar_semaphore_spec(Name, N) ->
	#{ id => Name
	 , start => {ar_semaphore, start_link, [Name, N]}
	 , type => worker
	 , shutdown => ?SHUTDOWN_TIMEOUT
	 }.
