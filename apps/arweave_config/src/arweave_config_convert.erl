%%% @doc Convert a legacy (pre-2.9.6) `config.json' file into the new
%%% JSON or YAML configuration format.
%%%
%%% The conversion reuses the production code paths so it stays correct
%%% as the option set evolves:
%%%
%%%   1. `arweave_config_format_legacy_json:parse/1' loads the legacy
%%%      file into the options registry, applying every legacy field
%%%      mapping, value transform, and feature-flag promotion.
%%%   2. The set of values that ended up in the store is read back and
%%%      re-encoded into the shapes the new-format reader expects
%%%      (addresses as base64url, peers as `host:port' strings, etc.).
%%%   3. `arweave_config_leaf_map:leaf_map_to_nested/1' rebuilds the
%%%      nested map and the matching `arweave_config_format_*:encode/1'
%%%      renders it.
%%%
%%% The store is snapshotted and restored around the parse, so the call
%%% leaves global configuration state untouched.
%%% The conversion should take place in a separate OS process anyway.
-module(arweave_config_convert).
-compile(warnings_as_errors).
-export([convert/3]).

%% @doc Read the legacy config at `InputFilename', convert it to
%% `Format' (`json' or `yaml'), and write the result to
%% `OutputFilename'.
-spec convert(Format, InputFilename, OutputFilename) -> Return when
	Format :: json | yaml | string() | binary(),
	InputFilename :: file:name_all(),
	OutputFilename :: file:name_all(),
	Return :: ok | {error, term()}.
convert(Format, InputFilename, OutputFilename) ->
	maybe
		{ok, Encoder} ?= encoder(Format),
		{ok, Data} ?= read_input(InputFilename),
		{ok, Nested} ?= legacy_to_nested(Data),
		{ok, Encoded} ?= Encoder:encode(Nested),
		write_output(OutputFilename, Encoded)
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Map the requested target format to its encoder module.
encoder(json) -> {ok, arweave_config_format_json};
encoder(yaml) -> {ok, arweave_config_format_yaml};
encoder(Format) when is_binary(Format) ->
	encoder_from_string(binary_to_list(Format));
encoder(Format) when is_list(Format) ->
	encoder_from_string(Format);
encoder(Format) ->
	{error, {unsupported_format, Format}}.

encoder_from_string(String) ->
	case string:lowercase(String) of
		"json" -> {ok, arweave_config_format_json};
		"yaml" -> {ok, arweave_config_format_yaml};
		"yml" -> {ok, arweave_config_format_yaml};
		_ -> {error, {unsupported_format, String}}
	end.

read_input(InputFilename) ->
	case file:read_file(InputFilename) of
		{ok, Data} -> {ok, Data};
		{error, Reason} -> {error, {read_input, InputFilename, Reason}}
	end.

write_output(OutputFilename, Encoded) ->
	case file:write_file(OutputFilename, Encoded) of
		ok -> ok;
		{error, Reason} -> {error, {write_output, OutputFilename, Reason}}
	end.

%% @doc Parse the legacy file into the registry, read the assembled
%% values back out as a nested config map, and restore the store to the
%% state it had before the call.
legacy_to_nested(Data) ->
	Snapshot = arweave_config_store:snapshot(),
	try
		%% Start from an empty store so the read-back reflects only the
		%% values this file produces.
		ok = arweave_config_store:restore([]),
		case arweave_config_format_legacy_json:parse(Data) of
			{ok, ok} ->
				arweave_config_leaf_map:leaf_map_to_nested(build_leaf_map());
			{error, Reason} ->
				{error, {parse_input, Reason}};
			{error, Reason, Item} ->
				{error, {parse_input, Reason, Item}}
		end
	after
		ok = arweave_config_store:restore(Snapshot)
	end.

%% @doc Build a per-leaf config map from every value the parse wrote
%% into the store, encoding each value into its new-format shape.
build_leaf_map() ->
	maps:from_list(
		[{Path, encode_leaf(Path, Value)}
		 || {Path, Value} <- arweave_config_store:items_with_prefix([]),
			keep_value(Value)]).

%% Sentinel "no value" markers never make it into the output.
keep_value(not_set) -> false;
keep_value(undefined) -> false;
keep_value(_) -> true.

%% @doc Encode one stored leaf value into the shape the new-format
%% reader round-trips. Encoding is driven by the option's spec type so
%% new options are handled automatically.
encode_leaf([join, start_from_block], Bin) when is_binary(Bin) ->
	%% Raw 48-byte block hash; the spec carries no type, so special-case.
	ar_util:encode(Bin);
encode_leaf(Path, Value) ->
	encode_typed(spec_type(Path), Path, Value).

%% Resolve the registered type for an option path, or `undefined' when
%% the path has no concrete spec (e.g. dotted literals).
spec_type(Path) ->
	case arweave_config_options_registry:resolve(Path) of
		{ok, _Option, Spec, _Bindings} -> maps:get(type, Spec, undefined);
		_ -> undefined
	end.

encode_typed(address, _Path, Bin) when is_binary(Bin) ->
	ar_util:encode(Bin);
encode_typed(Type, _Path, Peers)
		when (Type =:= resolved_peers_list orelse Type =:= peers_list),
			 is_list(Peers) ->
	[ar_util:format_peer(Peer) || Peer <- Peers];
encode_typed(resolved_peer_id, _Path, Peer) ->
	ar_util:format_peer(Peer);
encode_typed(list_map, Path, Items) when is_list(Items) ->
	[encode_list_item(Path, Item) || Item <- Items];
encode_typed(_Type, _Path, Value) ->
	encode_container(Value).

%% @doc Encode one element of a `list_map' value (a storage module,
%% webhook, ...), encoding each field by its own `{list_item}' spec.
encode_list_item(Root, Item) when is_map(Item) ->
	maps:from_list(
		[{Field, encode_field(Root, Field, Value)}
		 || {Field, Value} <- maps:to_list(Item)]);
encode_list_item(_Root, Item) ->
	encode_container(Item).

encode_field(Root, Field, Value) ->
	case spec_type(Root ++ [{list_item}, Field]) of
		address when is_binary(Value) -> ar_util:encode(Value);
		_ -> encode_container(Value)
	end.

%% @doc Turn a `{Key, Value}' proplist (e.g. webhook headers) into a map
%% so it serializes as a JSON/YAML object; pass everything else through
%% for the format encoder to handle.
encode_container(Value) when is_list(Value) ->
	case is_pair_list(Value) of
		true ->
			maps:from_list(
				[{Key, encode_container(Nested)} || {Key, Nested} <- Value]);
		false ->
			Value
	end;
encode_container(Value) ->
	Value.

is_pair_list([]) -> false;
is_pair_list(List) ->
	lists:all(fun({_, _}) -> true; (_) -> false end, List).
