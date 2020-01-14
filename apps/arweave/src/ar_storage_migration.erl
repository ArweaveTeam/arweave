-module(ar_storage_migration).

-export([start_link/1]).

-include("ar.hrl").

get_migrations() ->
	[{"indep_hash_only_block_filenames_v1", fun v1/1}].

start_link(HL) ->
	spawn_link(fun () -> run_migrations(HL, get_migrations()) end).

run_migrations(HL, [{Name, Fun} | Rest]) ->	
	MigrationsDir = filename:join(ar_meta_db:get(data_dir), ?STORAGE_MIGRATIONS_DIR),
	case filelib:is_file(filename:join(MigrationsDir, Name)) of
		true ->
			run_migrations(HL, Rest);
		false ->
			ok = run_migration(HL, {Name, Fun}),
			lists:foreach(fun(M) -> ok = run_migration(HL, M) end, Rest)
	end;
run_migrations(_HL, []) ->
	ok.

run_migration(HL, {Name, Fun}) ->
	MigrationsDir = filename:join(ar_meta_db:get(data_dir), ?STORAGE_MIGRATIONS_DIR),
	ar:info([{event, storage_migration}, {name, Name}]),
	ok = Fun(HL),
	MigrationFile = filename:join(MigrationsDir, Name),
	ok = filelib:ensure_dir(MigrationFile),
	ok = ar_storage:write_file_atomic(MigrationFile, <<>>),
	ar:info([{event, storage_migration_complete}, {name, Name}]).

v1(HL) ->
	BlocksDir = filename:join(ar_meta_db:get(data_dir), ?BLOCK_DIR),
	ok = v1(HL, length(HL) - 1, BlocksDir),
	ok.

v1([H | HL], Height, BlocksDir) when length(HL) == Height ->
	EncodedH = binary_to_list(ar_util:encode(H)),
	LegacyName = filename:join(
		BlocksDir,
		integer_to_list(Height) ++ "_" ++ EncodedH ++ ".json"
	),
	NewName = filename:join(
		BlocksDir,
		EncodedH ++ ".json"
	),
	case file:rename(LegacyName, NewName) of
		ok ->
			v1(HL, Height - 1, BlocksDir);
		{error, enoent} ->
			v1(HL, Height - 1, BlocksDir)
	end;
v1([], -1, _BlocksDir) ->
	ok;
v1(_HL, _Height, _BlocksDir) ->
	{error, height_does_not_match_hash_list}.
