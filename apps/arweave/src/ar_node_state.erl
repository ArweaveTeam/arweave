%%%
%%% @doc Server to maintain a node state.
%%%

-module(ar_node_state).

-export([start/0, stop/1, all/1, lookup/2, update/2]).

%%%
%%% Public API.
%%%

%% @doc Start a node state server.
start() ->
	Pid = spawn(fun() ->
		server(ets:new(ar_node_state, [set, private, {keypos, 1}]))
	end),
	% Set initial state values.
	update(Pid, [
		{id, crypto:strong_rand_bytes(32)}, % unique id of the ar_node
		{hash_list, not_joined},            % current full hashlist
		{current, not_joined},              % current block hash
		{wallet_list, []},                  % current up to date walletlist
		{height, 0},                        % current height of the blockweave
		{gossip, undefined},                % Gossip protcol state
		{txs, []},                          % set of new txs to be mined into the next block
		{miner, undefined},                 % PID of the mining process
		{mining_delay, 0},                  % delay on mining, used for netework simulation
		{automine, false},                  % boolean dictating if a node should automine
		{reward_addr, unclaimed},           % reward address for mining a new block
		{trusted_peers, []},                % set of trusted peers used to join on
		{waiting_txs, []},                  % set of txs on timeout whilst network distribution occurs
		{tags, []},                         % nodes tags to apply to a block when mining
		{reward_pool, 0},                   % current mining rewardpool of the weave
		{diff, 0},                          % current mining difficulty of the weave (no. of preceeding zero)
		{last_retarget, undefined},         % timestamp at which the last difficulty retarget occurred
		{weave_size, 0},                    % current size of the weave in bytes (only inc. data tx size)
		{cumulative_diff, 0},               % Sum of the difficulty squared along the current weave
		{hash_list_merkle, <<>>},           % The Merkle root of the current BHL
		{block_txs_pairs, []}               % List of {BH, TXIDs} pairs for last ?MAX_TX_ANCHOR_DEPTH blocks
	]),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Get all values from state, the return is a map of the keys
%% and values. The operation is atomic, all needed values must be
%% retrieved with one call. Between calls the state may change.
all(Pid) ->
	send(Pid, all).

%% @doc Get one or more values from state. In case of a single key
%% it will be returned as {ok, Value}, a non-existant key will return
%% {ok, undefined}. A list of keys will be returned as a map of
%% keys and values. The operation is atomic, all needed values must
%% be retrieved with one call. Between calls the state may change.
lookup(Pid, Keys) ->
	send(Pid, {lookup, Keys}).

%% @doc Set one or more values from state, input is a list of {Key, Value}
%% or a map. The operation is atomic, all needed values must be setted with
%% one call. Between calls the state may change.
update(Pid, KeyValues) ->
	send(Pid, {update, KeyValues}).

%%%
%%% Server functions.
%%%

%% @doc Send a message to the server and wait for the result.
send(Pid, Msg) ->
	Ref = make_ref(),
	Pid ! {?MODULE, Msg, self(), Ref},
	receive
		{?MODULE, Ref, Reply} ->
			Reply
	after
		5000 ->
			case Msg of
				{lookup, Keys} ->
					ar:warn([ar_node_state, lookup_timeout, {keys, Keys}]);
				{update, KeyValues} when is_list(KeyValues) ->
					ar:warn([ar_node_state, update_timeout, {keys, proplists:get_keys(KeyValues)}]);
				Other ->
					ar:warn([ar_node_state, msg_timeout, {message, Other}])
			end,
			{error, timeout}
	end.

%% @doc Main server loop.
server(Tid) ->
	receive
		{Module, Msg, From, Ref} ->
			try handle(Tid, Msg) of
				Reply ->
					From ! {Module, Ref, Reply},
					server(Tid)
			catch
				throw:Term ->
					ar:report( [ {'NodeStateEXCEPTION', Term } ]),
					server(Tid);
				exit:Term ->
					ar:report( [ {'NodeStateEXIT', Term} ] ),
					server(Tid);
				error:Term ->
					ar:report( [ {'NodeStateERROR', {Term, erlang:get_stacktrace()} } ]),
					server(Tid)
			end;
		stop ->
			ok
	end.

%% @doc Handle the individual server commands. Starving ets table has to be
%% avoided by any means. Only atoms are allowed as keys and changes have
%% to be done atomically.
handle(Tid, all) ->
	All = ets:match_object(Tid, '$1'),
	{ok, maps:from_list(All)};
handle(Tid, {lookup, Keys}) when is_list(Keys) ->
	case lists:all(fun is_atom/1, Keys) of
		true ->
			{ok, maps:from_list(lists:map(fun(Key) ->
				case ets:lookup(Tid, Key) of
					[{Key, Value}] -> {Key, Value};
					[]             -> {Key, undefined}
				end
			end, Keys))};
		_ ->
			{error, {invalid_node_state_keys, Keys}}
	end;
handle(Tid, {lookup, Key}) when is_atom(Key) ->
	case ets:lookup(Tid, Key) of
		[{Key, Value}] -> {ok, Value};
		[]             -> {ok, undefined}
	end;
handle(_Tid, {update, []}) ->
	ok;
handle(Tid, {update, KeyValues}) when is_list(KeyValues) ->
	case lists:all(fun({Key, _Value}) -> is_atom(Key) end, KeyValues) of
		true ->
			ets:insert(Tid, KeyValues),
			ok;
		_ ->
			{error, {invalid_node_state_keys, KeyValues}}
	end;
handle(Tid, {update, KeyValues}) when is_map(KeyValues) ->
	handle(Tid, {update, maps:to_list(KeyValues)});
handle(Tid, {update, {Key, Value}}) ->
	handle(Tid, {update, [{Key, Value}]});
handle(_Tid, {update, Any}) ->
	{error, {invalid_node_state_values, Any}};
handle(_Tid, {Command, Args}) ->
	{error, {invalid_node_state_command, {Command, Args}}}.
