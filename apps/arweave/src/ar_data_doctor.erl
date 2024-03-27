-module(ar_data_doctor).

-export([main/0, main/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

main() ->
	main([]).
main([]) ->
	help(),
	erlang:halt(1);
main(Args) ->
	logger:set_handler_config(default, level, error),
	Command = hd(Args),
	Success = case Command of
		"merge" ->
			ar_doctor_merge:main(tl(Args));
		"bench" ->
			ar_doctor_bench:main(tl(Args));
		"dump" ->
			ar_doctor_dump:main(tl(Args));
		_ ->
			false
	end,
	case Success of
		true ->
			erlang:halt(0);
		_ ->
			help(),
			erlang:halt(1)
	end.

help() ->
	ar:console("~n"),
	ar_doctor_merge:help(),
	ar:console("~n"),
	ar_doctor_bench:help(),
	ar:console("~n"),
	ar_doctor_dump:help(),
	ar:console("~n").

