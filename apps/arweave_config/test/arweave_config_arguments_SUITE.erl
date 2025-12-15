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
-module(arweave_config_arguments_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	default/1,
	parser/1
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
description() -> {description, "arweave config cli arguments interface"}.

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
init_per_testcase(_TestCase, Config) ->
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
		default,
		parser
	].

%%--------------------------------------------------------------------
%% @doc test `arweave_config_arguments' main interface.
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	{comment, "arguments process tested"}.

%%--------------------------------------------------------------------
%% @doc test `arweave_config_arguments' parser.
%% @end
%%--------------------------------------------------------------------
parser(_Config) ->
	% Create a custom long arguments map
	LongArguments = #{
		<<"--boolean">> => #{
			type => boolean
		},
		<<"--integer">> => #{
			type => integer
		},
		<<"--integer.pos">> => #{
			type => pos_integer
		}
	},

	% create a custom short argmuents map
	ShortArguments = #{
		$b => #{
			type => boolean
		},
		$i => #{
			type => integer
		},
		$I => #{
			type => pos_integer
		}
	},
	Arguments = [
		% long arguments
		<<"--boolean">>,
		<<"--boolean">>, <<"true">>,
		<<"--boolean">>, <<"false">>,
		<<"--boolean">>, <<"True">>,
		<<"--boolean">>, <<"TRUE">>,
		<<"--boolean">>, <<"FALSE">>,
		<<"--integer">>, <<"-65535">>,
		<<"--integer">>, <<"-0">>,
		<<"--integer">>, <<"-65535">>,
		<<"--integer.pos">>, <<"0">>,
		<<"--integer.pos">>, <<"65535">>,

		% short arguments
		<<"-b">>,
		<<"-b">>, <<"true">>,
		<<"-b">>, <<"on">>,
		<<"-b">>, <<"off">>,
		<<"-b">>, <<"false">>,
		<<"-i">>, <<"65535">>,
		<<"-i">>, <<"0">>,
		<<"-i">>, <<"-65535">>,
		<<"-I">>, <<"0">>,
		<<"-I">>, <<"65535">>
	],

	% --peer 127.0.0.1 --vdf --trusted
	% --storage.module 1.unpacked --enabled

	Opts = #{
		long_arguments => LongArguments,
		short_arguments => ShortArguments
	},

	ct:pal(test, 1, "parse ~p", [Arguments]),
	Result = [
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => integer},[-65535]},
		{#{type => integer},[0]},
		{#{type => integer},[-65535]},
		{#{type => pos_integer},[0]},
		{#{type => pos_integer},[65535]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[true]},
		{#{type => boolean},[false]},
		{#{type => boolean},[false]},
		{#{type => integer},[65535]},
		{#{type => integer},[0]},
		{#{type => integer},[-65535]},
		{#{type => pos_integer},[0]},
		{#{type => pos_integer},[65535]}
	],
	{ok, Result} = arweave_config_arguments:parse(Arguments, Opts),

	{comment, "arguments parser tested"}.

