-module(ar_storage_migration).

-export([run_migrations/1]).

-include("ar.hrl").

get_migrations() ->
	[{"indep_hash_only_block_filenames_v1", fun v1/1}].

run_migrations(HL) ->
	erlang:spawn_link(fun () -> run_migrations(HL, get_migrations()) end).

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
	lists:foldl(
		fun(H, Height) ->
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
					Height - 1;
				{error, enoent} ->
					Height - 1
			end		
		end,
		length(HL) - 1,
		HL
	),
	ok.
