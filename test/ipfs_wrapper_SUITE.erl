-module(ipfs_wrapper_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, 
	init_per_suite/1, end_per_suite/1,
	init_per_testcase/2, end_per_testcase/2
	]).

-export([
	]).

all() -> [
	].

%%%% set up

init_per_suite(Config) ->
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%% tests

%%% private

