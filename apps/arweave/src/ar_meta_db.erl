%% @doc Defines a small in-memory metadata table for Arweave nodes.
%% Typically used to store small peices of globally useful information
%% (for example: the port number used by the node).
%% @end

-module(ar_meta_db).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%% API
-export([start/0, stop/0, stop/1]).
-export([reset/0, reset_peer/1]).
-export([get/1, put/2, keys/0, purge_peer_performance/0, increase/2]).

%% Behaviour callbacks
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Starts the server
start() ->
	gen_server:start({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stops the server
stop() ->
	gen_server:stop(?SERVER).

%% @doc Stops the server with a reason
stop(Reason) ->
	StopTimeout = 5000, %% milliseconds
	gen_server:stop(?SERVER, Reason, StopTimeout).

%% @doc Deletes all objects in db
reset() ->
	gen_server:call(?SERVER, reset).

reset_peer(Peer) ->
	gen_server:call(?SERVER, {reset_peer, Peer}).

%% @doc Insert key-value-pair into db
put(Key, Value) ->
	gen_server:call(?SERVER, {put, Key, Value}).

%% @doc Retreive value for key
%% We don't want to serialize reads. So, we let
%% the calling process do the ets:lookup directly
%% without message passing. This is consistent with
%% how the ets table is created (public and
%% {read_concurrency, true}).
get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> Obj;
		[] -> not_found
	end.

%% @doc Increase the value associated with Key by Val. If the key
%% is not set, set it to Val.
increase(Key, Val) ->
	gen_server:cast(?SERVER, {increase, Key, Val}).

%% @doc Remove entries from the performance database older than ?PEER_TMEOUT
purge_peer_performance() ->
	gen_server:call(?SERVER, purge_peer_performance).

%% @doc Return all of the keys available in the database.
keys() ->
	gen_server:call(?SERVER, keys).

%%------------------------------------------------------------------------------
%% Behaviour callbacks
%%------------------------------------------------------------------------------

%% @hidden
init(_) ->
	%% Initialise the metadata storage service.
	ar:report([starting_meta_db]),
	ets:new(?MODULE, [set, public, named_table, {read_concurrency, true}]),
	ets:new(blacklist, [set, public, named_table]),
	{ok, #{}}.

%% @hidden
handle_call(reset, _From, State) ->
	ets:delete_all_objects(?MODULE),
	{reply, true, State};
%% @hidden
handle_call({reset_peer, Peer}, _From, State) ->
	ets:delete(?MODULE, {peer, Peer}),
	{reply, true, State};
handle_call({put, Key, Val}, _From, State) ->
	%% Put an Erlang term into the meta DB. Typically these are write-once values.
	ets:insert(?MODULE, {Key, Val}),
	{reply, true, State};
handle_call(purge_peer_performance, _From, State) ->
	purge_performance(),
	{reply, ok, State};
handle_call(keys, _From, State) ->
	Keys = ets:foldl(fun collect_keys/2, [], ?MODULE),
	{reply, Keys, State}.

%% @hidden
handle_cast({increase, Key, Val}, State) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, PrevVal}] -> ets:insert(?MODULE, {Key, PrevVal + Val});
		[] -> ets:insert(?MODULE, {Key, Val})
	end,
	{noreply, State};
handle_cast(_What, State) ->
	{noreply, State}.

%% @hidden
handle_info(_What, State) ->
	{noreply, State}.

%% @hidden
terminate(_Reason, _State) ->
	ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

purge_performance() ->
	ThresholdTime = os:system_time(seconds) - ?PEER_TIMEOUT,
	ets:safe_fixtable(?MODULE, true),
	purge_performance(ThresholdTime, ets:first(?MODULE)),
	ets:safe_fixtable(?MODULE, false),
	ok.

purge_performance(_, '$end_of_table') ->
	ok;
purge_performance(ThresholdTime, Key) ->
	[{_, Obj}] = ets:lookup(?MODULE, Key),
	case Obj of
		#performance{} when Obj#performance.timeout < ThresholdTime ->
			ets:delete(?MODULE, Key);
		_ ->
			%% The object might be something else than a performance record.
			noop
	end,
	purge_performance(ThresholdTime, ets:next(?MODULE, Key)).

collect_keys({Key, _Value}, Acc) ->
	[Key | Acc].

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

%% @doc Store and retreieve a test value.
basic_storage_test() ->
	reset(),
	?assertEqual(not_found, get(test_key)),
	put(test_key, test_value),
	?assertEqual(test_value, get(test_key)),
	?assertEqual(not_found, get(dummy_key)).

%% @doc Data older than ?PEER_TIMEOUT is removed, newer data is not
purge_peer_performance_test() ->
	CurrentTime = os:system_time(seconds),
	P1 = #performance{timeout = CurrentTime - (?PEER_TIMEOUT + 1)},
	P2 = #performance{timeout = CurrentTime - 1},
	Key1 = {peer, {127,1,2,3,1984}},
	Key2 = {peer, {127,1,2,3,1985}},
	put(Key1, P1),
	put(Key2, P2),
	put(some_config, 1984),
	purge_peer_performance(),
	?assertEqual(get(Key1), not_found),
	?assertEqual(get(Key2), P2),
	?assertEqual(get(some_config), 1984).
