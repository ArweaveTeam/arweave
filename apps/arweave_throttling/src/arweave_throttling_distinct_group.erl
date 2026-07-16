%%%===================================================================
%%% @doc Per-peer set of group ids.
%%%
%%% Stores the distinct group ids (binaries) seen for each `Peer' (a
%%% 5-tuple). Group ids are inserted one at a time; the primary query
%%% is "how many distinct group ids does this peer have". Backed by two
%%% ETS tables:
%%%
%%% <ul>
%%%   <li>`?MODULE' - membership set holding `{{Peer, GroupID}}'
%%%       objects. The composite key makes duplicate inserts of the
%%%       same `{Peer, GroupID}' pair no-ops. Created with
%%%       `read_concurrency'.</li>
%%%   <li>`?COUNTER_TABLE' - a `Peer -> distinct count' index kept in
%%%       sync with the membership set so `distinct_count/1' never
%%%       scans.</li>
%%% </ul>
%%% @end
%%%===================================================================
-module(arweave_throttling_distinct_group).

-export([
	init/0,
	insert/2,
	is_stored/2,
	distinct_count/1,
	cleanup/0
]).

-define(COUNTER_TABLE, arweave_throttling_distinct_group_counts).

%% @doc Create the membership and counter ETS tables. Called once
%% before any other function in this module.
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

%% @doc Record that `GroupID' was seen for `Peer'.
%%
%% Returns `{ok, new}' the first time a given `{Peer, GroupID}' pair is
%% inserted and `{ok, duplicate}' for any subsequent insert of the same
%% pair. Only `new' inserts bump the peer's distinct count.
-spec insert(Peer, GroupID) -> Result when
	Peer :: {term(), term(), term(), term(), term()},
	GroupID :: binary(),
	Result :: {ok, new | duplicate} | {error, term()}.
insert(Peer, GroupID)
  when is_binary(GroupID) ->
	try ets:insert_new(?MODULE, {{Peer, GroupID}}) of
		true ->
			incr_peer(Peer),
			{ok, new};
		false ->
			{ok, duplicate}
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Whether `GroupID' has already been stored for `Peer'.
%%
%% A pure read - never inserts. Returns `{ok, true}' when the
%% `{Peer, GroupID}' pair is present, `{ok, false}' otherwise.
-spec is_stored(Peer, GroupID) -> Result when
	Peer :: {term(), term(), term(), term(), term()},
	GroupID :: binary(),
	Result :: {ok, boolean()} | {error, term()}.
is_stored(Peer, GroupID)
  when is_binary(GroupID) ->
	try ets:member(?MODULE, {Peer, GroupID}) of
		IsStored ->
			{ok, IsStored}
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Number of distinct group ids stored for `Peer'.
%%
%% Returns `{ok, 0}' for a peer that has never had a group id inserted.
-spec distinct_count(Peer) -> Result when
	Peer :: {term(), term(), term(), term(), term()},
	Result :: {ok, non_neg_integer()} | {error, term()}.
distinct_count(Peer) ->
	try ets:lookup(?COUNTER_TABLE, Peer) of
		[{Peer, Count}] ->
			{ok, Count};
		[] ->
			{ok, 0}
	catch
		_:Reason ->
			{error, Reason}
	end.

%% @doc Delete both ETS tables, leaving no trace.
-spec cleanup() -> ok.
cleanup() ->
	catch ets:delete(?MODULE),
	catch ets:delete(?COUNTER_TABLE),
	ok.

%%%===================================================================
%%% Internals
%%%===================================================================

%% @doc Increment the distinct-group count for `Peer', creating the
%% entry at 0 first if needed.
incr_peer(Peer) ->
	ets:update_counter(?COUNTER_TABLE, Peer, 1, {Peer, 0}).
