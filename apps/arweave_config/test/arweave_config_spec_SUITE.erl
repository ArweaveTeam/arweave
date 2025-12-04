%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_spec_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([specs/1]).
-export([
	default/1,
	default_value/1,
	default_type/1,
	default_get/1,
	default_set/1,
	default_set_state/1,
	default_multi/1,
	default_runtime/1,
	default_multi_types/1,
	spec_environment/1,
	spec_long_argument/1
]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave configuration spec interface"}.

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
init_per_testcase(TestCase, Config) ->
	% required for runtime parameter
	ct:pal(info, 1, "start arweave_config"),
	{ok, _} = arweave_config:start_link(),

	% required for configuration storage
	ct:pal(info, 1, "start arweave_config_store"),
	{ok, PidStore} = arweave_config_store:start_link(),

	% required for specificatoin
	ct:pal(info, 1, "Start arweave_config_spec"),
	Specs = specs(TestCase),
	{ok, PidSpec} = arweave_config_spec:start_link(Specs),

	[
		{arweave_config_store, PidStore},
		{arweave_config_spec, PidSpec}
		| Config
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config_spec"),
	ok = arweave_config_spec:stop(),

	ct:pal(info, 1, "stop arweave_config_store"),
	ok = arweave_config_store:stop(),

	ct:pal(info, 1, "stop arweave_config"),
	ok = gen_server:stop(arweave_config).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		default,
		default_value,
		default_type,
		default_get,
		default_set,
		default_set_state,
		default_multi,
		default_runtime,
		default_multi_types,
		spec_environment,
		spec_long_argument
	].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	% default parameter only have all value by default, present
	% from the spec. In this case, it should return an error.
	{error, undefined} = arweave_config_spec:get([default]),

	% when setting a value, we should see the new value and the
	% old value. The value should also be present in the store
	{ok, test, undefined} = arweave_config_spec:set([default], test),
	{ok, test} = arweave_config_store:get([default]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_value(_Config) ->
	% a parameter with a default value
	{ok, true} = arweave_config_spec:get([default_value]),
	{error, undefined} = arweave_config_store:get([default_value]),
	{ok, false, true} = arweave_config_spec:set([default_value], false),
	{ok, false} = arweave_config_store:get([default_value]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_type(_Config) ->
	% a parameter with a defined type
	{ok, true, undefined} =
		arweave_config_spec:set([default_type], true),
	{error, _, _} =
		arweave_config_spec:set([default_type], "not a boolean").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_get(_Config) ->
	% a parameter with a specific get
	{ok, valid} =
		arweave_config_spec:get([default_get]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_set(_Config) ->
	% a parameter with a specific set
	{ok, ok, undefined} =
		arweave_config_spec:set([default_set], self()),
	ok = receive
		ok -> ok
	after
		10 -> error
	end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_set_state(_Config) ->
	% set state
	{ok, empty, undefined} =
		arweave_config_spec:set([default_set_state], ok),
	{ok, full, empty} =
		arweave_config_spec:set([default_set_state], ok),

	{comment, "set state tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_multi(_Config) ->
	% default value to 1
	ct:pal(test, 1, "set value to 1"),
	{ok, 1} = arweave_config_spec:get([one]),
	{ok, one, 1} = arweave_config_spec:set([one], one),

	% on default value, but always return 3
	ct:pal(test, 1, "get value"),
	{ok, 3} = arweave_config_spec:get([three]),
	{ok, 3, undefined} = arweave_config_spec:set([three], any),

	{comment, "multi spec tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_runtime(_Config) ->
	ct:pal(test, 1, "initial state not in runtime"),
	false = arweave_config:is_runtime(),
	{ok, 1, undefined} = arweave_config_spec:set([default], 1),
	{ok, 1, undefined} = arweave_config_spec:set([runtime], 1),
	{ok, 1, undefined} = arweave_config_spec:set([not_runtime], 1),

	ct:pal(test, 1, "swith to runtime"),
	ok = arweave_config:runtime(),
	true = arweave_config:is_runtime(),

	% by default, a parameter without runtime feature is not
	% allowed to be set during runtime
	{error, _} = arweave_config_spec:set([default], 2),

	% runtime parameter can be set during runtime
	{ok, 2, 1} = arweave_config_spec:set([runtime], 2),

	% not_runtime parameter can't be set during runtime
	{error, _} = arweave_config_spec:set([not_runtime], 2).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_multi_types(_Config) ->
	ct:pal(test, 1, "set a boolean"),
	{ok, true, undefined} =
		arweave_config_spec:set([default], <<"true">>),

	ct:pal(test, 1, "set an integer"),
	{ok, 1, true} =
		arweave_config_spec:set([default], 1),

	ct:pal(test, 1, "set an ipv4"),
	{ok, <<"127.0.0.1">>, 1} =
		arweave_config_spec:set([default], <<"127.0.0.1">>),

	{comment, "multi types tested"}.

%%--------------------------------------------------------------------
%% @doc test environment specification callback module.
%% @end
%%--------------------------------------------------------------------
spec_environment(_Config) ->
	% an environment can be any kind of binary
	{ok, #{ environment := <<"test">> }} =
		arweave_config_spec_environment:init(#{
				environment => <<"test">>
			},
			#{}
		),

	% an environment can be a list
	{ok, #{ environment := <<"AR_TEST">> }} =
		arweave_config_spec_environment:init(#{
				environment => "AR_TEST"
			},
			#{}
		),

	% an environment set to true will generate automatically
	% an environment variable using the parameter key prefixed
	% with AR.
	{ok, #{ environment := <<"AR_TEST">> }} =
		arweave_config_spec_environment:init(#{
				environment => true
			},
			#{
				parameter_key => [test]
			}
		),

	{ok, #{ environment := <<"AR_TEST_1_2_3_DATA">> }} =
		arweave_config_spec_environment:init(#{
				environment => true
			},
			#{
				parameter_key => [test,1,2,3,data]
			}
		),

	% an environment can't be a float.
	{error, _} =
		arweave_config_spec_environment:init(#{
				environment => 1.0
			},
			#{}
		),

	{comment, "environment spec tested"}.

%%--------------------------------------------------------------------
%% @doc test long argument specification callback module.
%% @end
%%--------------------------------------------------------------------
spec_long_argument(_Config) ->
	{ok, #{}} =
		arweave_config_spec_long_argument:init(#{}, #{}),

	{ok, #{ long_argument := <<"--test">> }} =
		arweave_config_spec_long_argument:init(#{
				long_argument => <<"test">>
			},
			#{}
		),

	{ok, #{ long_argument := <<"--test.data">> }} =
		arweave_config_spec_long_argument:init(#{
				long_argument => true
			},
			#{
				parameter_key => [test, data]
			}
		),

	{ok, #{ long_argument := <<"--test.1.data">> }} =
		arweave_config_spec_long_argument:init(#{
				long_argument => undefined
			},
			#{
				parameter_key => [test, 1, data]
			}
		),

	{ok, #{}} =
		arweave_config_spec_long_argument:init(#{
				long_argument => false
			},
			#{}
		),

	{comment, "long argument spec tested"}.

%%--------------------------------------------------------------------
%% @doc defines custom parameters for tests.
%% @end
%%--------------------------------------------------------------------
specs(default) ->
	[
		#{ parameter_key => [default] }
	];
specs(default_value) ->
	[
		#{
			parameter_key => [default_value],
			default => true
		 }
	];
specs(default_type) ->
	[
		#{
			parameter_key => [default_type],
			type => boolean
		}
	];
specs(default_get) ->
	[
		#{
			parameter_key => [default_get],
			handle_get => fun
				(K, _S) ->
					{ok, valid}
			end
		}
	];
specs(default_set) ->
	[
		#{
			parameter_key => [default_set],
			handle_set => fun
				(K, V, S) ->
					V ! ok,
					{ok, ok}
			end
		}
	];
specs(default_set_state) ->
	[
		#{
			parameter_key => [default_set_state],
			handle_set => fun
				(_K, _V, #{ config := Config }) ->
					case Config of
						#{ default_set_state := empty } ->
							{store, full};
						_ ->
							{store, empty}
					end
			end
		}
	];
specs(default_multi) ->
	[
		#{
			parameter_key => [one],
			default => 1
		},
		#{
			parameter_key => [three],
			handle_get => fun
				(_K, _S) ->
					{ok, 3}
			end,
			handle_set => fun
				(_K, _V, _S) ->
					{ok, 3}
			end
		}
	];
specs(default_runtime) ->
	[
		#{
			parameter_key => [default]
		},
		#{
			parameter_key => [runtime],
			runtime => true
		},
		#{
			parameter_key => [not_runtime],
			runtime => false
		}
	];
specs(default_multi_types) ->
	[
		#{
			parameter_key => [default],
			type => [boolean, integer, ipv4]
		}
	].

