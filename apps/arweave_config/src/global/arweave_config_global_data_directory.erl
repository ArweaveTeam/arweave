%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global_data_directory).
-behavior(arweave_config_spec).
-compile(export_all).

required() -> true.

legacy() -> [data_dir].

runtime() -> false.

short_description() -> <<"">>.

long_description() -> <<"">>.

configuration_key() -> [global,data,directory].

environment() -> "AR_DATA_DIRECTORY".

short_argument() -> $D.

long_argument() -> [data, directory].

elements() -> 1.

type() -> path.

check(Path) -> 
	case filelib:is_dir(Path) of
		true -> ok;
		false -> {error, not_directory}
	end.

handle_get(Key) -> {ok, value};

handle_set(Key, Value) -> {ok, Value}.
