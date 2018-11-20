-module(ipfs_wrapper_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, 
	init_per_suite/1, end_per_suite/1,
	init_per_testcase/2, end_per_testcase/2
	]).

-export([
	add_local_and_get/1
	]).

all() -> [
	add_local_and_get
	].

%%%% set up

init_per_suite(Config) ->
	{ok,_} = application:ensure_all_started(inets),
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(add_local_and_get, Config) ->
	Filename = "known_local.txt",
	DataDir = ?config(data_dir, Config),
	Path = DataDir ++ Filename,
	{ok, Data} = file:read_file(Path),
	[{add_local_data, {Data, Filename}} | Config];

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%% tests

add_local_and_get(Config) ->
	{Data, Filename} = ?config(add_local_data, Config),
	{ok, Hash} = ar_ipfs:add_data(Data, Filename),
	{ok, Data} = ar_ipfs:cat_data_by_hash(Hash).

%%% private

