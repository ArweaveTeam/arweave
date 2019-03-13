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
-export([start_link/0, stop/0, stop/1]).
-export([reset/0]).
-export([get/1, put/2, keys/0, remove_old/1, increase/2]).

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
start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stops the server
stop() ->
	gen_server:stop(?MODULE).

%% @doc Stops the server with a reason
stop(Reason) ->
	StopTimeout = 5000, %% milliseconds
	gen_server:stop(?MODULE, Reason, StopTimeout).

%% @doc Deletes all objects in db
reset() ->
	gen_server:call(?MODULE, reset).

%% @doc Insert key-value-pair into db
put(Key, Value) ->
	gen_server:call(?MODULE, {put, Key, Value}).

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

%% @doc Increase the value associated by a key by Val
increase(Key, Val) ->
	gen_server:call(?MODULE, {increase, Key, Val}).

%% @doc Remove entries from the performance database older than
%% ?PEER_TMEOUT
remove_old(Time) ->
	gen_server:call(?MODULE, {remove_old, Time}).

%% @doc Return all of the keys available in the database.
keys() ->
	gen_server:call(?MODULE, keys).

%%------------------------------------------------------------------------------
%% Behaviour callbacks
%%------------------------------------------------------------------------------

%% @hidden
init(_) ->
	%% @doc Initialise the metadata storage service.
	ar:report([starting_meta_db]),
	ets:new(?MODULE, [set, public, named_table, {read_concurrency, true}]),
	ets:new(blacklist, [set, public, named_table]),
	{ok, #{}}.

%% @hidden
handle_call(reset, _From, State) ->
	Res = ets:delete_all_objects(?MODULE),
	{reply, Res, State};

%% @hidden
handle_call({put, Key, Val}, _From, State) ->
	%% @doc Put an Erlang term into the meta DB. Typically these are
	%% write-once values.
	Res = ets:insert(?MODULE, {Key, Val}),
	{reply, Res, State};

%% @hidden
handle_call({increase, Key, Val}, _From, State) ->
	%% @doc Increase the value associated by a key by Val
	Res = case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> ets:insert(?MODULE, {Key, Obj + Val});
		[] -> not_found
	end,
	{reply, Res, State};

%% @hidden
handle_call({remove_old, Time}, _From, State) ->
	ets:safe_fixtable(?MODULE, true),
	Res = priv_remove_old(Time, ets:first(?MODULE)),
	ets:safe_fixtable(?MODULE, false),
	{reply, Res, State};

%% @hidden
handle_call(keys, _From, State) ->
	Keys = ets:foldl(fun collect_keys/2, [], ?MODULE),
	{reply, Keys, State}.

%% @hidden
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

priv_remove_old(_Time, '$end_of_table') ->
	done;

priv_remove_old(Time, Key) ->
	[{Key, P}] = ets:lookup(?MODULE, Key),
	Timeout = P#performance.timeout,
	TooOld = (Time - Timeout) >= ?PEER_TIMEOUT,
	priv_remove_old(Time, Key, Timeout, TooOld).

priv_remove_old(Time, Key, 0, _) ->
	priv_remove_old(Time, ets:next(?MODULE, Key));

priv_remove_old(Time, Key, _, true) ->
	true = ets:delete(?MODULE, Key),
	priv_remove_old(Time, ets:next(?MODULE, Key));

priv_remove_old(Time, Key, _, _ ) ->
	priv_remove_old(Time, ets:next(?MODULE, Key)).

collect_keys({Key, _Value}, Acc) ->
	[Key | Acc].

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

%% TODO: Right now, some tests are dependent on each other.
%% This means that we are not free to start and stop this module
%% as we please.

test_setup() ->
	ok.

test_teardown(_) ->
	reset(),
	ok.

%% @doc Store and retreieve a test value.
basic_storage_test_() ->
	{
		setup,
		fun test_setup/0,
		fun test_teardown/1,
		{
			inorder,
			[
				?_assertEqual(not_found, get(test_key)),
				?_assertEqual(true, put(test_key, test_value)),
				?_assertEqual(test_value, get(test_key)),
				?_assertEqual(not_found, get(dummy_key))
			]
		}
	}.

%% @doc Data older than ?PEER_TIMEOUT is removed, newer data is not
purge_old_peers_test_() ->
	Time = os:system_time(seconds),
	P1 = #performance{timeout = Time - (?PEER_TIMEOUT + 1)},
	P2 = #performance{timeout = Time - 1},
	Key1 = {peer, {127,0,0,1,1984}},
	Key2 = {peer, {127,0,0,1,1985}},
	{
		setup,
		fun test_setup/0,
		fun test_teardown/1,
		{
			inorder,
			[
				?_assert(put(Key1, P1) =:= true),
				?_assert(put(Key2, P2) =:= true),
				?_assert(remove_old(Time) =:= done),
				?_assert(get(Key1) =:= not_found),
				?_assert(get(Key2) =:= P2)
			]
		}
	}.
