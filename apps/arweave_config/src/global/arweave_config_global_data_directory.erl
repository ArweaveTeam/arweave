%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global_data_directory).
-behavior(arweave_config_spec).
-compile(export_all).

required() -> {ok, true}.

legacy() -> {ok, [data_dir]}.

runtime() -> {ok, false}.

short_description() -> {ok, <<"">>}.

long_description() -> {ok, <<"">>}.

configuration_key() -> {ok, [global,data,directory]}.

environment() -> {ok, "AR_DATA_DIRECTORY"}.

short_argument() -> {ok, $D}.

long_argument() -> {ok, [data, directory]}.

elements() -> {ok, 1}.

type() -> {ok, path}.

check(Path) -> 
	case filelib:is_dir(Path) of
		true -> ok;
		false -> {error, not_directory}
	end.

handle_get(Key) -> {ok, value}.

handle_set(Key, Value) -> {ok, Value}.
