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
	gen_server:stop(?SERVER).

%% @doc Stops the server with a reason
stop(Reason) ->
	StopTimeout = 5000, %% milliseconds
	gen_server:stop(?SERVER, Reason, StopTimeout).

%% @doc Deletes all objects in db
reset() ->
	gen_server:call(?SERVER, reset).

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

%% @doc Increase the value associated by a key by Val
increase(Key, Val) ->
	gen_server:call(?SERVER, {increase, Key, Val}).

%% @doc Remove entries from the performance database older than ?PEER_TMEOUT
remove_old(Time) ->
	gen_server:call(?SERVER, {remove_old, Time}).

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
	Res = ets:delete_all_objects(?MODULE),
	{reply, Res, State};
handle_call({put, Key, Val}, _From, State) ->
	%% Put an Erlang term into the meta DB. Typically these are write-once values.
	Res = ets:insert(?MODULE, {Key, Val}),
	{reply, Res, State};
handle_call({increase, Key, Val}, _From, State) ->
	%% Increase the value associated by a key by Val
	Res = case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> ets:insert(?MODULE, {Key, Obj + Val});
		[] -> not_found
	end,
	{reply, Res, State};
handle_call({remove_old, CurrentTime}, _From, State) ->
	Res = remove_old_performance(CurrentTime),
	{reply, Res, State};
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

remove_old_performance(CurrentTime) ->
	ThresholdTime = CurrentTime - ?PEER_TIMEOUT,
	ets:safe_fixtable(?MODULE, true),
	remove_old_performance(ThresholdTime, ets:first(?MODULE)),
	ets:safe_fixtable(?MODULE, false),
	done.

remove_old_performance(_, '$end_of_table') ->
	ok;
remove_old_performance(ThresholdTime, Key) ->
	[{_, Obj}] = ets:lookup(?MODULE, Key),
	case Obj of
		#performance{} when Obj#performance.timeout < ThresholdTime ->
			ets:delete(?MODULE, Key);
		_ ->
			%% The object might be something else than a performance record.
			noop
	end,
	remove_old_performance(ThresholdTime, ets:next(?MODULE, Key)).

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
				?_assert(put(port, 1984) =:= true),
				?_assert(remove_old(Time) =:= done),
				?_assert(get(Key1) =:= not_found),
				?_assert(get(Key2) =:= P2),
				?_assert(get(port) =:= 1984)
			]
		}
	}.
