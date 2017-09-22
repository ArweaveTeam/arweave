-module(ar_logging).
-export([save_log/1]).
-include("ar_network_tests.hrl").

%%% Manages logging of large scale, long term, nightly network tests
%%% found in ar_network_tests.

%% Save a log object from a test monitor to a file.
save_log(T) ->
	file:write_file(
		Filename = generate_filename(T),
		io_lib:format(
			"Name: ~p~nStart time: ~p~nFail time: ~p~n~n~s~n",
			[
				T#test_run.name,
				T#test_run.start_time,
				T#test_run.fail_time,
				lists:flatten(format_logs(lists:reverse(T#test_run.log)))
			]
		)
	),
	lists:flatten(Filename).

%% Turn a #test_run into a reasonable file name.
generate_filename(
	#test_run {
		name = Name,
		start_time = {{Yr, Mo, Da}, {Hr, Mi, Se}}
	}) ->
	io_lib:format("~s/~p_~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b.log",
		[?LOG_DIR, Name, Yr, Mo, Da, Hr, Mi, Se]
	).

%% Output a string representing a log.
format_logs([]) -> "";
format_logs([[{B, _}]|Logs]) ->
	io_lib:format("No forks. Block height: ~p.~n", [B#block.height])
		++ format_logs(Logs);
format_logs([Log|Logs]) ->
	io_lib:format("Fork detected:~n", []) ++
		string:join(
			lists:map(
				fun({B, Num}) ->
					io_lib:format(
						"\tBlock: ~p~n\t\tHeight: ~p~n\t\tNodes: ~p~n",
						[
							B#block.hash,
							B#block.height,
							Num
						]
					)
				end,
				Log
			),
			[$\n]
		) ++
		format_logs(Logs).
