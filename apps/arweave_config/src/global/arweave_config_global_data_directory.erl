%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global_data_directory).
-behavior(arweave_config_spec).
-export([
	configuration_key/0,
	required/0,
	legacy/0,
	runtime/0,
	environment/0,
	short_argument/0,
	long_argument/0,
	short_description/0,
	long_description/0,
	elements/0,
	type/0,
	check/1,
	handle_get/1,
	handle_set/2
]).

configuration_key() -> {ok, [global, data, directory]}.

runtime() -> false.

required() -> {ok, true}.

legacy() -> {ok, [data_dir]}.

short_description() -> {ok, <<"">>}.

long_description() -> {ok, <<"">>}.

environment() -> {ok, <<"AR_DATA_DIRECTORY">>}.

short_argument() -> {ok, <<"">>}.

long_argument() -> {ok, [data, directory]}.

elements() -> {ok, 1}.

type() -> {ok, path}.

check(Path) -> 
	case filelib:is_dir(Path) of
		true -> ok;
		false -> {error, not_directory}
	end.

handle_get(_Key) -> {ok, value}.

handle_set(_Key, Value) -> {ok, Value}.
