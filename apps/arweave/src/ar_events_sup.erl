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

%% Helper macro for declaring children of supervisor.
-define(CHILD(Mod, I, Type), {I, {Mod, start_link, [I]}, permanent, 30000, Type, [Mod]}).

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
		%% Events: new, ready_for_mining, dropped.
		?CHILD(ar_events, tx, worker),
		%% Events: discovered, rejected, new, mined.
		?CHILD(ar_events, block, worker),
		%% Events: unpack_request, unpacked, repack_request, packed.
		?CHILD(ar_events, chunk, worker),
		%% Events: made_request, bad_response, served_tx, served_block, served_chunk,
		%% gossiped_tx, gossiped_block, banned
		?CHILD(ar_events, peer, worker),
		%% Events: initializing, initialized, validated_pre_fork_2_7_block, new_tip,
		%% checkpoint_block, search_space_upper_bound.
		?CHILD(ar_events, node_state, worker),
		%% Events: initialized, valid, invalid, computed_output.
		?CHILD(ar_events, nonce_limiter, worker),
		%% Events: found_solution.
		?CHILD(ar_events, miner, worker),
		%% Events: removed_file.
		?CHILD(ar_events, chunk_storage, worker),
		%% Events: add_range, remove_range, cut.
		?CHILD(ar_events, data_sync, worker),
		%% Used for the testing purposes.
		?CHILD(ar_events, testing, worker)
	]}}.
