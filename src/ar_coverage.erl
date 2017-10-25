-module(ar_coverage).
-export([analyse/1]).
%%% Module with functions used to calculate code coverage for eunit tests.

%% @doc
analyse(Fun) ->
  cover:compile_directory("src"),
  Fun(),
  case cover:analyse(coverage, line) of
    {result, Reslist, []} ->
      print_results(Reslist);
    _ -> ar:console_report({coverage_error, Fun})
  end,
  cover:stop().

print_results(Reslist) ->
  Cov = lists:sum([ 1 || { _, { 1, _ }} <- Reslist ]) / length(Reslist),
  ar:report_console([{total_coverage_percentage, 100*Cov}]).
