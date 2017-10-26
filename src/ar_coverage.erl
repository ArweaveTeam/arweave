-module(ar_coverage).
-export([analyse/1]).

%%% Module with functions used to calculate code coverage.

%% @doc Take a function, execute it, and print the coverage data.
analyse(Fun) ->
	cover:compile_directory("src"),
	Fun(),
	case cover:analyse(coverage, line) of
		{result, Reslist, []} -> print_results(Reslist);
		_ -> ar:console_report({coverage_error, Fun})
	end,
	cover:stop().

%% @doc Report the results of a coverage run.
print_results(ResList) ->
	io:format("=======================================================~n"),
	io:format("Total coverage: ~.2f%~n", [calculate_coverage(ResList)]),
	lists:foreach(
		fun(Mod) ->
			io:format(
				"~s: ~.2f%~n",
				[
					string:right(atom_to_list(Mod), 20, $ ),
					calculate_coverage(ResList, Mod)
				]
			)
		end,
		ar_util:unique([ Mod || {{Mod, _}, _} <- ResList ])
	).

%% @doc Format and print a coverage result for a single module.
calculate_coverage(ResList, Mod) ->
	calculate_coverage([ Res || Res = {{XMod, _}, _} <- ResList, XMod == Mod ]).
calculate_coverage(ResList) ->
	(lists:sum([ 1 || {_, { 1, _ }} <- ResList ]) / length(ResList)) * 100.
