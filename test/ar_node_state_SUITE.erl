%%%
%%% @doc Unit tests of the node state process.
%%%

-module(ar_node_state_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%%
%%% ct callbacks.
%%%

%% @doc All tests of this suite.
all() ->
	[
		{group, success},
		{group, fail}
	].

%% @doc Groups of tests.
groups() ->
	[
		{success, [parallel], [
			update_success,
			update_and_all_success,
			update_and_lookup_success
		]},
		{fail, [parallel], [
			update_fail,
			lookup_fail
		]}
	].

%%%
%%% Tests.
%%%

%% @doc Successful updates.
update_success(_Config) ->
	{ok, Pid} = ar_node_state:start(),
	ok = ar_node_state:update(Pid, []),
	ok = ar_node_state:update(Pid, [{a, 1}, {b, 2}, {c, 3}]),
	ok = ar_node_state:update(Pid, [{b, 9}, {d, 8}]),
	ok = ar_node_state:update(Pid, {e, 7}),
	ok = ar_node_state:update(Pid, {f, [1, 2, 3, 4, 5]}),
	ok = ar_node_state:update(Pid, #{a => 99, g => "test"}),
	ok = ar_node_state:stop(Pid).

%% @doc Successful updates and access of all.
update_and_all_success(_Config) ->
	{ok, Pid} = ar_node_state:start(),
	{ok, All} = ar_node_state:all(Pid),
	% 18 reflect number of standard node state fields.
	18 = maps:size(All),
	ok = ar_node_state:stop(Pid).

%% @doc Successful updates and lookups.
update_and_lookup_success(_Config) ->
	{ok, Pid} = ar_node_state:start(),
	ok = ar_node_state:update(Pid, [{a, 1}, {b, 2}, {c, 3}]),
	{ok, V1} = ar_node_state:lookup(Pid, [a, c]),
	#{a := 1, c := 3} = V1,
	ok = ar_node_state:update(Pid, {f, [1, 2, 3, 4, 5]}),
	{ok, V2} = ar_node_state:lookup(Pid, f),
	#{f := [1, 2, 3, 4, 5]} = V2,
	{ok, V3} = ar_node_state:lookup(Pid, zz),
	#{zz := undefined} = V3,
	{ok, V4} = ar_node_state:lookup(Pid, [c, a, zz, f]),
	#{a := 1, c := 3, f := [1, 2, 3, 4, 5], zz := undefined} = V4,
	ok = ar_node_state:stop(Pid).

%% @doc Failing updates.
update_fail(_Config) ->
	{ok, Pid} = ar_node_state:start(),
	{error, {invalid_node_state_keys, [{a, 1}, {42, <<"foo">>}]}} =
		ar_node_state:update(Pid, [{a, 1}, {42, <<"foo">>}]),
	{error, {invalid_node_state_values, <<"invalid">>}} =
		ar_node_state:update(Pid, <<"invalid">>),
	ok = ar_node_state:stop(Pid).

%% @doc Failing lookups.
lookup_fail(_Config) ->
	{ok, Pid} = ar_node_state:start(),
	{error, {invalid_node_state_keys, [1]}} =
		ar_node_state:lookup(Pid, 1),
	{error, {invalid_node_state_keys, [a, b, {invalid}]}} =
		ar_node_state:lookup(Pid, [a, b, {invalid}]),
	ok = ar_node_state:stop(Pid).

%%%
%%% EOF
%%%

