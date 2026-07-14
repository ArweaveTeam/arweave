%%% @doc Arweave Configuration Leaf Map Test Suite.
-module(arweave_config_leaf_map_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

suite() ->
	[{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:start(),
	[].

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		flatten_nested_value,
		set_conflict,
		leaf_map_to_nested,
		merge_nested_maps,
		leaf_map_to_nested_with_opts,
		deeply_nested_map_roundtrip,
		numeric_keys,
		empty_map_roundtrip
	].

%%====================================================================
%% Test cases
%%====================================================================

%% set/3 recurses into map and list-of-maps Values, extending the Path
%% prefix per key, and atomizes binary/charlist keys via convert_key/1.
%% The only production caller (arweave_config_format_dotted:walk_top)
%% always passes a single-segment Path — these cases mirror that with
%% [k] so the leaf-map keys read like the real ones.
flatten_nested_value(_Config) ->
	{ok, #{}} = arweave_config_leaf_map:set([k], #{}, #{}),

	{ok, #{[k, 1] := 1}} =
		arweave_config_leaf_map:set([k], #{1 => 1}, #{}),

	{ok, #{[k, test] := 2}} =
		arweave_config_leaf_map:set([k], #{<<"test">> => 2}, #{}),

	{ok, #{[k, test] := 3}} =
		arweave_config_leaf_map:set([k], #{"test" => 3}, #{}),

	{ok, #{[k, 1, 2, 3] := 4}} =
		arweave_config_leaf_map:set([k], #{1 => #{2 => #{3 => 4}}}, #{}),

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
		[k, data, directory] => <<"/path/to/data">>,
		[k, data, 1] => 2,
		[k, data, a] => b,
		[k, logging, debug, enabled] => true,
		[k, <<"random_uRxsNKiM">>, <<"random_gblL5sdA">>] => [],
		[k, test, test, test, data] => test
	},
	{ok, Result} = arweave_config_leaf_map:set([k], Map, #{}),
	ok.

%% Same path with the same value is idempotent; same path with a
%% different value returns conflicting_config_key.
set_conflict(_Config) ->
	{ok, LeafMap} = arweave_config_leaf_map:set([a, b], 1, #{}),
	{ok, LeafMap} = arweave_config_leaf_map:set([a, b], 1, LeafMap),
	?assertMatch(
		{error, #{reason := conflicting_config_key}},
		arweave_config_leaf_map:set([a, b], 2, LeafMap)),
	ok.

leaf_map_to_nested(_Config) ->
	{ok, #{}} = arweave_config_leaf_map:leaf_map_to_nested(#{}),

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
	}} = arweave_config_leaf_map:leaf_map_to_nested(#{
		[1] => 1,
		[1, 2] => test,
		[1, 3] => data,
		[t, 1, test] => data
	}),

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
	{ok, Result} = arweave_config_leaf_map:leaf_map_to_nested(#{
		[1, 2, 3] => 4,
		[1, 2, 3, a] => b
	}),

	ok.

merge_nested_maps(_Config) ->
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
	?assertEqual(Result, arweave_config_leaf_map:merge_nested_maps([
		#{1 => 1},
		#{1 => #{2 => test}},
		#{1 => #{3 => data}},
		#{t => #{1 => #{test => data}}}
	])),
	ok.

leaf_map_to_nested_with_opts(_Config) ->
	LeafMap = #{[a, b] => 1},

	{ok, R1} = arweave_config_leaf_map:leaf_map_to_nested(LeafMap, #{}),
	#{a := #{b := 1}} = R1,

	{ok, R2} = arweave_config_leaf_map:leaf_map_to_nested(LeafMap, #{compact => true}),
	#{a := #{b := 1}} = R2,

	ok.

deeply_nested_map_roundtrip(_Config) ->
	Deep = #{
		a => #{
			b => #{
				c => #{
					d => #{
						e => #{
							f => leaf
						}
					}
				}
			}
		}
	},

	{ok, LeafMap} = arweave_config_leaf_map:set([], Deep, #{}),
	#{[a, b, c, d, e, f] := leaf} = LeafMap,

	{ok, Nested} = arweave_config_leaf_map:leaf_map_to_nested(LeafMap),
	?assertEqual(Deep, Nested),

	ok.

numeric_keys(_Config) ->
	Map = #{1 => #{2 => #{3 => value}}},

	{ok, LeafMap} = arweave_config_leaf_map:set([], Map, #{}),
	#{[1, 2, 3] := value} = LeafMap,

	{ok, Nested} = arweave_config_leaf_map:leaf_map_to_nested(LeafMap),
	?assertEqual(Map, Nested),

	ok.

empty_map_roundtrip(_Config) ->
	{ok, #{}} = arweave_config_leaf_map:set([], #{}, #{}),

	{ok, #{}} = arweave_config_leaf_map:leaf_map_to_nested(#{}),

	{ok, LeafMap} = arweave_config_leaf_map:set([], #{}, #{}),
	{ok, Nested} = arweave_config_leaf_map:leaf_map_to_nested(LeafMap),
	#{} = Nested,

	ok.
