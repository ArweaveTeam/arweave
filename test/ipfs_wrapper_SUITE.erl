-module(ipfs_wrapper_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, 
	init_per_suite/1, end_per_suite/1,
	init_per_testcase/2, end_per_testcase/2
	]).

-export([
	get_known_local/1
	]).

all() -> [
	get_known_local
	].

%%%% set up

init_per_suite(Config) ->
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(get_known_local, Config) ->
	{Hash, Data} = add_known_local(Config),
	[{known_local, {Hash, Data}} | Config];

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%% tests

get_known_local(Config) ->
	{Hash, Data} = ?config(known_local, Config),
	Data = ipfs:get_data_by_hash(Hash).

%%% private

add_known_local(Config) ->
	Fn = "known_local.txt",
	DataDir = ?config(data_dir, Config),
	Path = DataDir ++ "/" ++ Fn,
	{ok, Data} = file:read_file(Path),
	Hash = ipfs:add(Path),
	{Hash, Data}.
