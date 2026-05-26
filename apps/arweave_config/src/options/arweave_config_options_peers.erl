%%% @doc Specs for the `peers` option group. Options for
%%% describing peer relationships and the roles they serve.
%%%
%%% The peer config is structured as `[peers, <peer_id>, <role>]`
%%% where each role is a boolean leaf. This module also exposes the
%%% legacy-list bridge helpers and the peer-set validator that
%%% enforces `cm_exit` singleton, `vdf_client` / `vdf_server` mutual
%%% exclusion per peer, and at-least-one-role per peer.
-module(arweave_config_options_peers).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([
	roles/0,
	clear_role/1,
	write_legacy_list/2,
	write_legacy_singleton/2,
	by_role/1,
	singleton_by_role/1
]).
-include("arweave_config.hrl").
-define(DEFAULT_PEER_PORT, 1984).

specs() ->
	[
		%% A single peer can hold any combination of these role
		%% booleans. Cross-peer invariants (cm_exit singleton, vdf
		%% client/server exclusivity, at-least-one-role) are enforced
		%% by `validate/0` below.
		#{
			enabled => true,
			option_key => [peers, {peer_id}, trusted],
			default => false,
			type => boolean,
			short_description =>
				<<"Peer is a trusted/joining peer (sync, header "
				  "fetch).">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, block_gossip],
			default => false,
			type => boolean,
			short_description =>
				<<"Peer is a target for outgoing block gossip.">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, local],
			default => false,
			type => boolean,
			short_description =>
				<<"Peer is on the local network (no rate limiting "
				  "between us).">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, cm_peer],
			default => false,
			type => boolean,
			short_description =>
				<<"Peer to mine in coordination with.">>,
			long_description =>
				<<"You need to also set coordinated_mining, "
				  "cm_api_secret, and cm_exit_peer. The same peer "
				  "may be specified as cm_peer and cm_exit_peer. "
				  "If we are an exit peer, make sure to also set "
				  "cm_peer for every miner we work with.">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, cm_exit],
			default => false,
			type => boolean,
			short_description =>
				<<"Peer to send mining solutions to in coordinated "
				  "mining mode.">>,
			long_description =>
				<<"You need to also set coordinated_mining, "
				  "cm_api_secret, and cm_peer. If cm_exit_peer is "
				  "not set, we are the exit peer. When is_pool_client "
				  "is set, the exit peer is a proxy through which we "
				  "communicate with the pool. At most one peer may "
				  "carry this role.">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, vdf_client],
			default => false,
			type => boolean,
			short_description =>
				<<"Push VDF updates to this peer.">>,
			long_description =>
				<<"You can specify several vdf_client peers. "
				  "Mutually exclusive with vdf_server on the same "
				  "peer.">>
		},
		#{
			enabled => true,
			option_key => [peers, {peer_id}, vdf_server],
			default => false,
			type => boolean,
			short_description =>
				<<"Trusted VDF server peer to receive VDF updates "
				  "from.">>,
			long_description =>
				<<"If the option is set, we expect the given peer(s) "
				  "to push VDF updates to us; we will thus not compute "
				  "VDF outputs ourselves. Recommended on CPUs without "
				  "hardware extensions for computing SHA-2. We will "
				  "nevertheless validate VDF chains in blocks. We "
				  "recommend specifying at least two trusted peers to "
				  "aim for shorter mining downtime. Mutually exclusive "
				  "with vdf_client on the same peer.">>
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
%% the peer_id store. Each entry in the list becomes a
%% `[peers, PeerID, Role] = true` write. Existing per-peer entries
%% under the same role are cleared first — this is a replace, not an
%% append.
-spec write_legacy_list(atom(), [tuple() | binary() | string()]) -> ok.
write_legacy_list(Role, List) when is_list(List) ->
	clear_role(Role),
	[ok = write_one(Role, Peer) || Peer <- List],
	ok.

%% @doc Write a legacy singleton value (`cm_exit_peer`) into the
%% peer_id store. `not_set` clears any existing peer carrying the role.
-spec write_legacy_singleton(atom(), tuple() | binary() | string() | not_set) ->
	ok.
write_legacy_singleton(Role, not_set) ->
	clear_role(Role),
	ok;
write_legacy_singleton(Role, Peer) ->
	clear_role(Role),
	write_one(Role, Peer).

%% @doc Delete every `[peers, _, Role]` leaf currently set to `true`.
clear_role(Role) ->
	[arweave_config_store:delete([peers, PeerID, Role])
		|| PeerID <- peers_with_role(Role)],
	ok.

write_one(Role, Peer) ->
	case arweave_config_type:peer_id(Peer) of
		{ok, PeerID} ->
			%% set_local bypasses the gen_server — required because
			%% this code runs from inside the spec gen_server's
			%% handle_set callback, where a normal set/2 would
			%% deadlock.
			_ = arweave_config_options_registry:set_local(
				[peers, PeerID, Role], true),
			ok;
		{error, _Reason} ->
			%% Skip malformed entries silently.
			ok
	end.

%% @doc Read the peer_id store and rebuild the legacy flat list for a
%% given role.
%%
%% The role may be passed as an atom or a binary; binaries go through
%% `binary_to_existing_atom/1` so unknown role names fail fast.
%%
%% Peers whose `peer_id` parses as IPv4 are returned in the legacy
%% `{A, B, C, D, Port}` tuple shape; hostnames are returned as
%% binaries.
-spec by_role(atom() | binary()) -> [tuple() | binary()].
by_role(Role) when is_binary(Role) ->
	by_role(binary_to_existing_atom(Role));
by_role(Role) when is_atom(Role) ->
	[peer_id_to_legacy(PeerID) || PeerID <- peers_with_role(Role)].

%% @doc Read the peer_id store and return a single peer carrying
%% `Role`, or `not_set` if none does. The role may be passed as an
%% atom or a binary; binaries go through `binary_to_existing_atom/1`
%% so unknown role names fail fast.
-spec singleton_by_role(atom() | binary()) -> tuple() | binary() | not_set.
singleton_by_role(Role) when is_binary(Role) ->
	singleton_by_role(binary_to_existing_atom(Role));
singleton_by_role(Role) when is_atom(Role) ->
	case peers_with_role(Role) of
		[] -> not_set;
		[PeerID | _] -> peer_id_to_legacy(PeerID)
	end.

%% @doc Return the list of peer_ids that have `Role = true` in the
%% store.
peers_with_role(Role) ->
	Items = arweave_config:get_all_with_prefix([peers]),
	[PeerID
		|| {[peers, PeerID, R], true} <- Items, R =:= Role].

%% @doc Convert a normalized `<<"host:port">>` peer_id back into the
%% legacy `{A, B, C, D, Port}` tuple shape. Bare IPv4 (no port) is
%% promoted to the default port. Hostnames pass through unchanged.
peer_id_to_legacy(PeerID) when is_binary(PeerID) ->
	case binary:split(PeerID, <<":">>, [global]) of
		[Host, PortBin] ->
			Port = binary_to_integer(PortBin),
			case parse_ipv4(Host) of
				{ok, {A, B, C, D}} -> {A, B, C, D, Port};
				error -> PeerID
			end;
		[Host] ->
			case parse_ipv4(Host) of
				{ok, {A, B, C, D}} -> {A, B, C, D, ?DEFAULT_PEER_PORT};
				error -> PeerID
			end
	end.

parse_ipv4(Host) ->
	try
		Parts = binary:split(Host, <<".">>, [global]),
		case [binary_to_integer(P) || P <- Parts] of
			[A, B, C, D] when A >= 0, A =< 255, B >= 0, B =< 255,
					C >= 0, C =< 255, D >= 0, D =< 255 ->
				{ok, {A, B, C, D}};
			_ ->
				error
		end
	catch
		_:_ -> error
	end.

%% @doc Peer-set validator. Enforces:
%%
%%   - at most one `cm_exit` peer;
%%   - `vdf_client` and `vdf_server` mutually exclusive per peer;
%%   - every peer carries at least one `true` role.
-spec validate() -> ok | {error, term()}.
validate() ->
	Items = arweave_config:get_all_with_prefix([peers]),
	%% Group items by peer_id.
	ByPeer = lists:foldl(
		fun
			({[peers, PeerID, Role], true}, Acc) ->
				maps:update_with(PeerID,
					fun(Roles) -> [Role | Roles] end,
					[Role],
					Acc);
			(_, Acc) ->
				Acc
		end,
		#{},
		Items
	),

	case validate_cm_exit_singleton(ByPeer) of
		{error, _} = Err -> Err;
		ok ->
			case validate_vdf_exclusivity(ByPeer) of
				{error, _} = Err -> Err;
				ok ->
					validate_at_least_one_role(ByPeer, Items)
			end
	end.

validate_cm_exit_singleton(ByPeer) ->
	WithCMExit = [ID || {ID, Roles} <- maps:to_list(ByPeer),
		lists:member(cm_exit, Roles)],
	case length(WithCMExit) of
		N when N =< 1 -> ok;
		_ -> {error, {multiple_cm_exit_peers, WithCMExit}}
	end.

validate_vdf_exclusivity(ByPeer) ->
	Conflicts = [ID || {ID, Roles} <- maps:to_list(ByPeer),
		lists:member(vdf_client, Roles),
		lists:member(vdf_server, Roles)],
	case Conflicts of
		[] -> ok;
		_ -> {error, {vdf_client_and_server_on_same_peer, Conflicts}}
	end.

validate_at_least_one_role(ByPeer, AllItems) ->
	%% A peer with only `false` leaves is present in the store but
	%% has no active role — reject it.
	AllPeerIDs = lists:usort(
		[ID || {[peers, ID, _], _V} <- AllItems]),
	Empty = [ID || ID <- AllPeerIDs,
		not maps:is_key(ID, ByPeer)],
	case Empty of
		[] -> ok;
		_ -> {error, {peer_with_no_roles, Empty}}
	end.

