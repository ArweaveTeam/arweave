%%% @doc Arweave configuration help generator.
-module(arweave_config_help).
-compile(warnings_as_errors).
-export([main/0, main/1, print/0, print_group/1]).

main() ->
	main([]).

main([]) ->
	print(),
	erlang:halt(0);
main([GroupName]) when is_list(GroupName) ->
	print_group(GroupName),
	erlang:halt(0);
main(_) ->
	io:format("Usage: arweave config help [group]~n"),
	erlang:halt(1).

%% @doc Default help: short intro + per-group summary
%% (key, default, short description on one line).
-spec print() -> ok.
print() ->
	print_intro(),
	lists:foreach(fun print_group_summary/1, grouped_parameters()),
	ok.

%% @doc Detailed help for a single group: per-option key, long
%% description, default, runtime flag.
-spec print_group(string()) -> ok.
print_group(GroupName) when is_list(GroupName) ->
	try list_to_existing_atom(GroupName) of
		GroupAtom -> do_print_group(GroupAtom)
	catch
		error:badarg -> unknown_group(GroupName)
	end.

%%%===================================================================
%%% Intro + group summary (default help).
%%%===================================================================

print_intro() ->
	io:format(
		"Each option key can be set four ways:~n"
		"  CLI flag:           --option.key value~n"
		"  JSON / YAML:        option.key: value~n"
		"  Environment var:    AR_OPTION_KEY=value~n"
		"~n"
		"Run `arweave config help <group>` for detailed help on "
		"a specific group.~n"
		"~n"
		"Available option groups:~n~n").

print_group_summary({Group, Description, Options}) ->
	io:format("=== ~ts ===~n", [atom_to_binary(Group)]),
	case Description of
		undefined -> ok;
		_ -> io:format("~ts~n", [Description])
	end,
	io:nl(),
	lists:foreach(fun print_option_summary/1, Options),
	io:nl().

print_option_summary(Option) ->
	io:format("  ~ts (default: ~ts) - ~ts~n",
		[key_string(Option), default_string(Option), short_desc(Option)]).

%%%===================================================================
%%% Per-group detailed help.
%%%===================================================================

do_print_group(GroupAtom) ->
	case lists:keyfind(GroupAtom, 1, grouped_parameters()) of
		false ->
			unknown_group(atom_to_list(GroupAtom));
		{GroupAtom, Description, Options} ->
			io:format("=== ~ts ===~n", [atom_to_binary(GroupAtom)]),
			case Description of
				undefined -> ok;
				_ -> io:format("~ts~n", [Description])
			end,
			io:nl(),
			lists:foreach(fun print_option_detail/1, Options)
	end.

print_option_detail(Option) ->
	Runtime = atom_to_binary(maps:get(runtime, Option, false)),
	io:format("  ~ts~n", [key_string(Option)]),
	io:format("    ~ts~n", [long_desc(Option)]),
	io:format("    default: ~ts~n", [default_string(Option)]),
	io:format("    runtime: ~ts~n", [Runtime]),
	io:nl().

unknown_group(GroupName) ->
	io:format("Unknown group: ~s~n~n", [GroupName]),
	io:format("Available groups:~n"),
	lists:foreach(
		fun({G, _, _}) -> io:format("  ~s~n", [atom_to_list(G)]) end,
		grouped_parameters()),
	erlang:halt(1).

%%%===================================================================
%%% Spec collection.
%%%===================================================================

%% @doc Cluster visible options by their `group' tag in
%% `group_order/0' order. Each group's options are sorted by key.
grouped_parameters() ->
	Visible = visible_parameters(),
	Groups = lists:foldl(
		fun(Option, Acc) ->
			Group = maps:get(group, Option, misc),
			Description = maps:get(group_description, Option, undefined),
			case maps:get(Group, Acc, undefined) of
				undefined ->
					Acc#{Group => {Description, [Option]}};
				{ExistingDescription, ExistingOptions} ->
					Acc#{Group => {ExistingDescription,
						[Option | ExistingOptions]}}
			end
		end, #{}, Visible),
	GroupOrder = arweave_config_options_spec:group_order(),
	lists:filtermap(
		fun(Group) ->
			case maps:find(Group, Groups) of
				{ok, {Desc, Options}} ->
					{true, {Group, Desc, sort_by_key(Options)}};
				error -> false
			end
		end, GroupOrder).

visible_parameters() ->
	[
		Option
		|| Option <- arweave_config_options_spec:all(),
		   maps:get(enabled, Option, true) =/= false
	].

sort_by_key(Options) ->
	lists:sort(
		fun(A, B) -> key_string(A) =< key_string(B) end,
		Options).

%%%===================================================================
%%% Field accessors.
%%%===================================================================

key_string(#{option_key := Key}) ->
	arweave_config_parser:format_key(Key).

default_string(Option) ->
	iolist_to_binary(
		io_lib:format("~0tp", [maps:get(default, Option, undefined)])).

%% Prefer short_description; fall back to long_description.
short_desc(Option) ->
	case nonempty(maps:get(short_description, Option, undefined)) of
		{ok, V} -> V;
		none ->
			case nonempty(maps:get(long_description, Option, undefined)) of
				{ok, V} -> V;
				none -> <<"No description available.">>
			end
	end.

%% Prefer long_description; fall back to short_description.
long_desc(Option) ->
	case nonempty(maps:get(long_description, Option, undefined)) of
		{ok, V} -> V;
		none ->
			case nonempty(maps:get(short_description, Option, undefined)) of
				{ok, V} -> V;
				none -> <<"No description available.">>
			end
	end.

nonempty(undefined) -> none;
nonempty(<<>>) -> none;
nonempty("") -> none;
nonempty(V) -> {ok, V}.
