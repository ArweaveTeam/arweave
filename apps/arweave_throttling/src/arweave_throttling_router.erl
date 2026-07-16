%%%===================================================================
%%% @doc Peer/path -> group-id routing table.
%%%
%%% Maps a `{Peer, PathKey}' key to the throttling group id that should
%%% handle requests for that peer and path. Backed by two ETS tables:
%%%
%%% <ul>
%%%   <li>`?MODULE' - the routing table proper, holding
%%%       `{{Peer, PathKey}, GroupID}' objects. Created with
%%%       `read_concurrency' since lookups dominate.</li>
%%%   <li>`?COUNTER_TABLE' - a `GroupID -> count' index kept in sync
%%%       with the routing table so `info/0' can report per-group key
%%%       counts without scanning the whole routing table.</li>
%%% </ul>
%%% @end
%%%===================================================================
-module(arweave_throttling_router).

-export([
	init/0,
	update_path/3,
	lookup_path/2,
	delete_path/2,
	info/0,
	cleanup/0
]).

-define(COUNTER_TABLE, arweave_throttling_router_counters).

%% @doc Create the routing and counter ETS tables. Called once before
%% any other function in this module.
-spec init() -> ok.
init() ->
	?MODULE = ets:new(?MODULE, [
		named_table,
		set,
		public,
		{read_concurrency, true}
	]),
	?COUNTER_TABLE = ets:new(?COUNTER_TABLE, [
		named_table,
		set,
		public
	]),
	ok.

%% @doc Route `{Peer, PathKey}' to `GroupID'.
%%
%% Returns `{ok, new}' when no group was stored for the key,
%% `{ok, unchanged}' when the stored group already equals `GroupID',
%% and `{ok, changed}' when an existing group was replaced.
-spec update_path(Peer, PathKey, GroupID) -> Result when
	Peer :: term(),
	PathKey :: term(),
	GroupID :: term(),
	Result :: {ok, new | unchanged | changed} | {error, term()}.
update_path(Peer, PathKey, GroupID) ->
	Key = {Peer, PathKey},
	try ets:lookup(?MODULE, Key) of
		[] ->
			true = ets:insert(?MODULE, {Key, GroupID}),
			incr_group(GroupID),
			{ok, new};
		[{Key, GroupID}] ->
			{ok, unchanged};
		[{Key, OldGroupID}] ->
			true = ets:insert(?MODULE, {Key, GroupID}),
			decr_group(OldGroupID),
			incr_group(GroupID),
			{ok, changed}
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Look up the group id routed for `{Peer, PathKey}'.
%%
%% Returns `{error, unknown_key}' when the key is absent.
-spec lookup_path(Peer, PathKey) -> Result when
	Peer :: term(),
	PathKey :: term(),
	Result :: {ok, term()} | {error, unknown_key | term()}.
lookup_path(Peer, PathKey) ->
	Key = {Peer, PathKey},
	try ets:lookup(?MODULE, Key) of
		[{Key, GroupID}] ->
			{ok, GroupID};
		[] ->
			{error, unknown_key}
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Delete the routing entry for `{Peer, PathKey}', if any.
-spec delete_path(Peer, PathKey) -> ok | {error, term()} when
	Peer :: term(),
	PathKey :: term().
delete_path(Peer, PathKey) ->
	Key = {Peer, PathKey},
	try ets:lookup(?MODULE, Key) of
		[{Key, GroupID}] ->
			true = ets:delete(?MODULE, Key),
			decr_group(GroupID),
			ok;
		[] ->
			ok
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Report table statistics.
%%
%% ```
%% #{keys_total     => TotalKeys,
%%   group_ids_total => DistinctGroups,
%%   keys_per_group => #{GroupID => KeyCount, ...}}
%% '''
%%
%% `keys_per_group' is read from the counter table, so this does not
%% scan the routing table.
-spec info() -> #{
		keys_total => non_neg_integer(),
		group_ids_total => non_neg_integer(),
		keys_per_group => #{term() => pos_integer()}
	}.
info() ->
	KeysPerGroup = ets:foldl(
		fun({GroupID, Count}, Acc) -> Acc#{GroupID => Count} end,
		#{},
		?COUNTER_TABLE
	),
	#{
		keys_total => ets:info(?MODULE, size),
		group_ids_total => map_size(KeysPerGroup),
		keys_per_group => KeysPerGroup
	}.

%% @doc Delete both ETS tables, leaving no trace.
-spec cleanup() -> ok.
cleanup() ->
	catch ets:delete(?MODULE),
	catch ets:delete(?COUNTER_TABLE),
	ok.

%%%===================================================================
%%% Internals
%%%===================================================================

%% @doc Increment the key count for `GroupID', creating the entry at 0
%% first if needed.
incr_group(GroupID) ->
	ets:update_counter(?COUNTER_TABLE, GroupID, 1, {GroupID, 0}).

%% @doc Decrement the key count for `GroupID', removing the entry once
%% it reaches zero so `info/0' never reports empty groups.
decr_group(GroupID) ->
	case ets:update_counter(?COUNTER_TABLE, GroupID, -1, {GroupID, 0}) of
		N when N =< 0 ->
			ets:delete(?COUNTER_TABLE, GroupID),
			ok;
		_ ->
			ok
	end.
