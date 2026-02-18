%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2026 (c) Arweave
%%% @doc Arweave Configuration File Serializer Test Suite.
%%% @end
%%%===================================================================
-module(arweave_config_serializer_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([encoder/1,decoder/1,map_merge/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave config parameters bootstrap"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_suite(Config) -> Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_suite(_Config) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	[].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		encoder,
		decoder,
		map_merge
	].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
encoder(_Config) ->
	ct:pal(test, 1, "check encoder with empty value"),
	{ok, #{}} = arweave_config_serializer:encode(#{}),

	ct:pal(test, 1, "check encoder with 1 key"),
	{ok, #{
		[1] := 1
	}} = arweave_config_serializer:encode(#{ 1 => 1 }),

	ct:pal(test, 1, "check encoder with binary key"),
	{ok, #{
		[test] := 2
	}} = arweave_config_serializer:encode(#{ <<"test">> => 2 }),

	ct:pal(test, 1, "check encoder with list key"),
	{ok, #{
		[test] := 3
	}} = arweave_config_serializer:encode(#{ "test" => 3 }),

	ct:pal(test, 1, "check encoder with recursive map"),
	{ok, #{
		[1,2,3] := 4
	}} = arweave_config_serializer:encode(#{ 1 => #{ 2 => #{ 3 => 4 }}}),

	ct:pal(test, 1, "full encoding test"),
	Map = #{
		<<"data">> => #{
			<<"directory">> => <<"/path/to/data">>,
			1 => 2,
			a => b
		},
		<<"logging">> => #{
			<<"debug">> => #{
				<<"enabled">> => true
			}
		},
		<<"random_uRxsNKiM">> => #{
			<<"random_gblL5sdA">> => []
		},
		"test" => #{
			<<"test">> => #{
				test => #{
					  data => test
				}
			}
		}
	},
	Result = #{
		[data,directory] => <<"/path/to/data">>,
		[data,1] => 2,
		[data,a] => b,
		[logging,debug,enabled] => true,
		[<<"random_uRxsNKiM">>, <<"random_gblL5sdA">>] => [],
		[test,test,test,data] => test
	},
	{ok, Result} = arweave_config_serializer:encode(Map),

	{comment, "encoder tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
decoder(_Config) ->
	ct:pal(test, 1, "check decoder with empty value"),
	{ok, #{}} = arweave_config_serializer:decode(#{}),

	ct:pal(test, 1, "check decoder with complex value"),
	{ok, #{
		1 := #{
			'_' := 1,
			2 := test,
			3 := data
		},
		t := #{
			1 := #{
				test := data
			}
		}
	}} = arweave_config_serializer:decode(#{
		[1] => 1,
		[1,2] => test,
		[1,3] => data,
		[t,1,test] => data
	}),

	ct:pal(test, 1, "full decoding test"),
	Result = #{
		1 => #{
			2 => #{
		       		3 => #{
				       '_' => 4,
				       a => b
				}
			}
		}
	},
	{ok, Result} = arweave_config_serializer:decode(#{
		[1,2,3] => 4,
		[1,2,3,a] => b
	}),

	{comment, "decoder tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
map_merge(_Config) ->
	ct:pal(test, 1, "test merge map"),
	Result = #{
		1 => #{
		       '_' => 1,
			2 => test,
			3 => data
		},
		t => #{
		       1 => #{
			      test => data
			}
		}
	},
	Result = arweave_config_serializer:map_merge([
		#{ 1 => 1 },
		#{ 1 => #{ 2 => test } },
		#{ 1 => #{ 3 => data } },
		#{ t => #{ 1 => #{ test => data }}}
	]),
	{comment, "map merger tested"}.
