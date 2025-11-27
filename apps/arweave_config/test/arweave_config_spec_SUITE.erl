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
	default_inherit/1,
	default_environment/1
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
		default_inherit,
		default_environment
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
%% @doc
%% @end
%%--------------------------------------------------------------------
default_inherit(_Config) ->
	ct:pal(test, 1, "check inheritance"),
	{ok, true} = arweave_config:get([default]),
	{ok, true} = arweave_config:get([inherit,all]),
	{ok, 1} = arweave_config:get([inherit,nothing]),

	ct:pal(test, 1, "check specs from ets"),
	[{_, #{ type := boolean, default := true}}] =
		ets:lookup(arweave_config_spec, [default]),

	[{_, #{ type := boolean, default := true}}] =
		ets:lookup(arweave_config_spec, [inherit, all]),

	[{_, #{ type := boolean }}] =
		ets:lookup(arweave_config_spec, [inherit, type]),

	[{_, #{ default := true }}] =
		ets:lookup(arweave_config_spec, [inherit, default]),

	[{_, #{ default := 1, type := pos_integer }}] =
		ets:lookup(arweave_config_spec, [inherit, nothing]),

	{comment, "inherit feature tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default_environment(_Config) ->
	ct:pal(test, 1, "get the list of environment variables"),
	Env = arweave_config_spec:get_environments(),

	ct:pal(test, 1, "check disabled environment"),
	false = lists:search(fun
		({_, [environment,disabled]}) -> true;
		(_) -> false end,
		Env
	),

	ct:pal(test, 1, "check enabled environment (generated)"),
	{value, {<<"AR_ENVIRONMENT_ENABLED">>, [environment,enabled]}} =
		lists:search(fun
			({_,[environment,enabled]}) -> true;
			(_) -> false
		end,
		Env
	),

	ct:pal(test, 1, "check custom enabled environment"),
	{value, {<<"CUSTOM">>, [environment,custom]}} =
		lists:search(fun
			({_,[environment,custom]}) -> true;
			(_) -> false
		end,
		Env
	),

	{comment, "environment feature tested"}.

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
				(K, V, S, _) ->
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
				(_K, _V, #{ config := Config }, _) ->
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
				(_K, _V, _S, _) ->
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
	];
specs(default_inherit) ->
	[
		#{
			parameter_key => [default],
			type => boolean,
			default => true
		},
		#{
			parameter_key => [inherit,all],
			inherit => [default]
		},
		#{
			parameter_key => [inherit,type],
			inherit => {[default], [type]}
		},
		#{
			parameter_key => [inherit,default],
			inherit => {[default], [default]}
		},
		#{
			parameter_key => [inherit,nothing],
			inherit => [default],
			type => pos_integer,
			default => 1
		}
	];
specs(default_environment) ->
	[
		#{
			parameter_key => [environment,disabled],
			environment => false
		},
		#{
			parameter_key => [environment,enabled],
			environment => true
		},
		#{
			parameter_key => [environment,custom],
			environment => <<"CUSTOM">>
		}
	].
