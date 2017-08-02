-module(app_chirper).
-export([start/0, start/1, chirp/4, get_chirps/2]).
-export([new_transaction/2, message/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A de-centralised, uncensorable microblogging platform.
%%% For examplary purposes only.

%% The tag to use for for new chirps.
-define(CHIRP_TAG, "chirper").

%% Define the state record for a chirper node.
-record(state, {
	db = [] % Stores the 'database' of links to chirps.
}).

-record(chirp, {
	tx_id,
	text
}).

%% Start a chirper node.
start() -> start([]).
start(Peers) ->
	adt_simple:start(?MODULE, #state{}, Peers).

%% Check to see if a new transaction contains a chirp,
%% store it if it does.
new_transaction(S, T) ->
	case lists:member(?CHIRP_TAG, T#tx.tags) of
		true ->
			% Transaction contains a chirp! Update the state.
			S#state { db = add_chirp(S#state.db, T) };
		false ->
			% Transaction isn't for us. Return the state unchanged.
			S
	end.

%% Handle non-gossip server requests.
message(S, {get_chirps, ResponsePID, PubKey}) ->
	ResponsePID ! {chirps, PubKey, find_chirps(S#state.db, PubKey)},
	S.

%% Store a chirp in the database.
%% For simplicity, we are currently using a key value list of public keys.
add_chirp(DB, T) ->
	NewChirp = #chirp { tx_id = T#tx.id, text = bitstring_to_list(T#tx.data) },
	case lists:keyfind(T#tx.owner, 1, DB) of
		false -> [{T#tx.owner, [NewChirp]}|DB];
		{PubKey, Chirps} ->
			lists:keyreplace(PubKey, 1, DB, {PubKey, [NewChirp|Chirps]})
	end.

%% Find chirps associated with a public key and return them.
find_chirps(DB, PubKey) ->
	case lists:keyfind(PubKey, 1, DB) of
		{PubKey, Chirps} -> Chirps;
		false -> []
	end.

%% Submit a new chirp to the system. Needs a peer to send the new transaction
%% to, as well as the private and public key of the wallet sending the tx.
%% NOTE: In a 'real' system, the transaction would be prepared by the server
%% then sent to the user's web browser extension for signing (after the user
%% agrees), limiting private key's exposure. Even here the private key is not
%% exposed to the application server.
chirp(Pub, Priv, Text, Peer) ->
	% Generate the transaction.
	TX = ar_tx:new(list_to_bitstring(Text)),
	% Add the chirper tg to the TX
	PreparedTX = TX#tx { tags = [?CHIRP_TAG] },
	% Sign the TX with the public and private key.
	SignedTX = ar_tx:sign(PreparedTX, Priv, Pub),
	ar_node:add_tx(Peer, SignedTX).

%% Get the chirps that a server knows.
get_chirps(Server, PubKey) ->
	Server ! {get_chirps, self(), PubKey},
	receive
		{chirps, PubKey, Chirps} -> Chirps
	end.

%% Test that a chirp submitted to the network is found and can be retreived.
basic_usage_test() ->
	% Spawn a network with two nodes and a chirper server
	ChirpServer = start(),
	Peers = ar_network:start(100, 10),
	ar_node:add_peers(hd(Peers), ChirpServer),
	% Create the transaction, send it.
	{Priv, Pub} = ar_wallet:new(),
	chirp(Pub, Priv, "Hello world!", hd(Peers)),
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 500 -> ok end,
	[Chirp] = get_chirps(ChirpServer, Pub),
	"Hello world!" = Chirp#chirp.text.
