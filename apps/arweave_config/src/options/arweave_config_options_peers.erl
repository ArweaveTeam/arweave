%%% @doc Specs for the `peers` option group. Options for
%%% describing peer relationships and the roles they serve.
%%%
%%% The peer config is structured as `[peers, <role>] => [peer_id()]`.
%%% This module also exposes legacy-list bridge helpers for old config
%%% formats and the peer-set validator that enforces `vdf_client` /
%%% `vdf_server` mutual exclusion per peer.
-module(arweave_config_options_peers).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([
	roles/0,
	clear_role/1,
	write_legacy_list/2,
	write_legacy_singleton/2,
	by_role/1,
	singleton_by_role/1,
	peers_with_role/1
]).
-include("arweave_config.hrl").
-define(DEFAULT_PEER_PORT, 1984).

specs() ->
	[
		#{
			enabled => true,
			option_key => [peers, trusted],
			default => [],
			type => resolved_peers_list,
			short_description =>
				<<"Trusted/joining peers used for sync and header fetch.">>
		},
		#{
			enabled => true,
			option_key => [peers, block_gossip],
			default => [],
			type => resolved_peers_list,
			short_description =>
				<<"Targets for outgoing block gossip.">>
		},
		#{
			enabled => true,
			option_key => [peers, local],
			default => [],
			type => resolved_peers_list,
			short_description =>
				<<"Peers on the local network.">>
		},
		#{
			enabled => true,
			option_key => [peers, cm_peer],
			default => [],
			type => resolved_peers_list,
			short_description =>
				<<"Peers to mine in coordination with.">>
		},
		#{
			enabled => true,
			option_key => [peers, cm_exit],
			default => not_set,
			type => resolved_peer_id,
			short_description =>
				<<"Peer to send mining solutions to in coordinated mining mode.">>
		},
		#{
			enabled => true,
			option_key => [peers, vdf_client],
			default => [],
			type => peers_list,
			short_description =>
				<<"Peers to push VDF updates to.">>
		},
		#{
			enabled => true,
			option_key => [peers, vdf_server],
			default => [],
			type => peers_list,
			short_description =>
				<<"Trusted VDF server peers to receive VDF updates from.">>
		}
	].

group_description() ->
	<<"Define peer roles and trust relationships.">>.

%% @doc List of all role atoms supported for each configured peer.
-spec roles() -> [atom()].
roles() ->
	[trusted, block_gossip, local, cm_peer, cm_exit,
	 vdf_client, vdf_server].

%% @doc Write a legacy list value (e.g. the parsed `peers` list) into
%% the role-list store. Existing entries under the same role are
%% replaced.
-spec write_legacy_list(atom(), [tuple() | binary() | string()]) -> ok.
write_legacy_list(Role, List) when is_list(List) ->
	PeerIDs = normalize_peers(List),
	_ = arweave_config_options_registry:set_local([peers, Role], PeerIDs),
	ok.

%% @doc Write a legacy singleton value (`cm_exit_peer`) into the
%% peer_id store. `not_set` clears any existing peer carrying the role.
-spec write_legacy_singleton(atom(), tuple() | binary() | string() | not_set) ->
	ok.
write_legacy_singleton(Role, not_set) ->
	_ = arweave_config_options_registry:set_local([peers, Role], not_set),
	ok;
write_legacy_singleton(Role, Peer) ->
	case arweave_config_type:peer_id(Peer) of
		{ok, PeerID} ->
			_ = arweave_config_options_registry:set_local([peers, Role], PeerID),
			ok;
		{error, _Reason} ->
			ok
	end.

%% @doc Delete the stored list for `Role`.
clear_role(Role) ->
	_ = arweave_config_store:delete([peers, Role]),
	ok.

normalize_peers(Peers) ->
	lists:usort([PeerID || Peer <- Peers,
		{ok, PeerID} <- [arweave_config_type:peer_id(Peer)]]).

%% @doc Read the canonical peer_ids for a given role (`{A, B, C, D,
%% Port}' for IPv4, `<<"host:port">>' binary for hostnames / IPv6).
%% The role may be an atom or a binary; binaries go through
%% `binary_to_existing_atom/1' so unknown role names fail fast.
-spec by_role(atom() | binary()) -> [tuple() | binary()].
by_role(Role) when is_binary(Role) ->
	by_role(binary_to_existing_atom(Role));
by_role(Role) when is_atom(Role) ->
	peers_with_role(Role).

%% @doc Read the peer_id store and return a single peer carrying
%% `Role`, or `not_set` if none does. The role may be passed as an
%% atom or a binary; binaries go through `binary_to_existing_atom/1'
%% so unknown role names fail fast.
-spec singleton_by_role(atom() | binary()) -> tuple() | binary() | not_set.
singleton_by_role(Role) when is_binary(Role) ->
	singleton_by_role(binary_to_existing_atom(Role));
singleton_by_role(Role) when is_atom(Role) ->
	case arweave_config_options_registry:get_local([peers, Role]) of
		{ok, not_set} -> not_set;
		{ok, [PeerID | _]} -> PeerID;
		{ok, PeerID} when is_tuple(PeerID); is_binary(PeerID) -> PeerID;
		_ -> not_set
	end.

%% @doc Return the list of peer_ids that have `Role` set in the store.
peers_with_role(cm_exit) ->
	case arweave_config_options_registry:get_local([peers, cm_exit]) of
		{ok, not_set} -> [];
		{ok, PeerID} when is_tuple(PeerID); is_binary(PeerID) -> [PeerID];
		_ -> []
	end;
peers_with_role(Role) ->
	case arweave_config_options_registry:get_local([peers, Role]) of
		{ok, PeerIDs} when is_list(PeerIDs) -> PeerIDs;
		_ -> []
	end.

%% @doc Peer-set validator. Enforces that `vdf_client` and
%% `vdf_server` are mutually exclusive per peer.
-spec validate() -> ok | {error, term()}.
validate() ->
	Items = arweave_config:get_all_with_prefix([peers]),
	ByPeer = lists:foldl(
		fun({[peers, Role], PeerIDs}, Acc) when is_list(PeerIDs) ->
			lists:foldl(
				fun(PeerID, PeerAcc) ->
					maps:update_with(PeerID,
						fun(Roles) -> [Role | Roles] end,
						[Role],
						PeerAcc)
				end,
				Acc,
				PeerIDs);
			(_, Acc) ->
				Acc
		end,
		#{},
		Items
	),
	validate_vdf_exclusivity(ByPeer).

validate_vdf_exclusivity(ByPeer) ->
	Conflicts = [ID || {ID, Roles} <- maps:to_list(ByPeer),
		lists:member(vdf_client, Roles),
		lists:member(vdf_server, Roles)],
	case Conflicts of
		[] -> ok;
		_ -> {error, {vdf_client_and_server_on_same_peer, Conflicts}}
	end.
