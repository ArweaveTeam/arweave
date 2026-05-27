%%% @doc Specs for the `join` option group. Options for
%%% deciding where a node begins syncing from.
-module(arweave_config_options_join).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0, normalize/0]).
-export([set_start_from_block/4]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [join, auto],
			default => true,
			type => boolean,
			legacy => auto_join,
			short_description =>
				<<"Automatically join the network of your peers at "
				  "startup.">>,
			long_description =>
				<<"Legacy CLI / JSON `no_auto_join` sets this to "
				  "false.">>
		},
		#{
			enabled => true,
			option_key => [join, workers],
			default => ?DEFAULT_JOIN_WORKERS,
			type => pos_integer,
			legacy => join_workers,
			short_description =>
				<<"Number of workers fetching recent blocks and "
				  "transactions simultaneously when joining the "
				  "network.">>
		},
		#{
			enabled => true,
			option_key => [join, start_from_latest_state],
			default => false,
			type => boolean,
			legacy => start_from_latest_state,
			short_description =>
				<<"Start the node from the latest stored state.">>
		},
		#{
			enabled => true,
			option_key => [join, start_from_state],
			default => not_set,
			runtime => false,
			type => string,
			legacy => start_from_state,
			required => false,
			short_description =>
				<<"Start the node from the state stored in the "
				  "specified folder.">>,
			long_description =>
				<<"This folder must be different from data_dir. "
				  "Implicitly sets start_from_latest_state to "
				  "true.">>
		},
		#{
			enabled => true,
			option_key => [join, start_from_block],
			default => not_set,
			legacy => start_from_block,
			short_description =>
				<<"Start the node from the state corresponding to "
				  "the given block hash.">>,
			long_description =>
				<<"Accepts a URL-safe base64 string or the raw 48-byte "
				  "binary.">>,
			handle_set =>
				fun arweave_config_options_join:set_start_from_block/4
		}
	].

%% @doc Cross-cutting: also reads [data_dir].
validate() ->
	case arweave_config:get([join, start_from_state]) of
		not_set ->
			ok;
		Folder ->
			DataDir = arweave_config:get([data_dir]),
			%% Coerce both sides to strings since `path` specs return
			%% binaries, while the legacy field was a string.
			FolderS = to_string(Folder),
			DataDirS = to_string(DataDir),
			case filename:absname(FolderS) == filename:absname(DataDirS) of
				true ->
					{error, <<"start_from_state folder cannot be the same as "
							"data_dir.">>};
				false ->
					ok
			end
	end.

to_string(B) when is_binary(B) -> binary_to_list(B);
to_string(L) when is_list(L) -> L.

%% @doc If `start_from_state` is set, force `start_from_latest_state` on
%% so the resume path uses the supplied folder.
normalize() ->
	case arweave_config:get([join, start_from_state]) of
		not_set -> ok;
		undefined -> ok;
		_ -> _ = arweave_config:set([join, start_from_latest_state], true), ok
	end.

group_description() ->
	<<"Control node startup and network join behavior.">>.

%% @doc `start_from_block` transform: accepts a raw 48-byte binary or a
%% URL-safe base64 string that decodes to 48 bytes. The `not_set`
%% sentinel passes through untouched.
set_start_from_block(_K, V, _S, _A) ->
	case decode_base64_with_size(V, 48) of
		{ok, Decoded} -> {store, Decoded};
		{error, _} -> {error, {bad_start_from_block, V}}
	end.

decode_base64_with_size(not_set, _Size) ->
	{ok, not_set};
decode_base64_with_size(B, Size) when is_binary(B), byte_size(B) =:= Size ->
	{ok, B};
decode_base64_with_size(B, Size) when is_binary(B) ->
	try b64fast:decode(B) of
		Decoded when byte_size(Decoded) =:= Size -> {ok, Decoded};
		_ -> {error, bad_size}
	catch
		_:_ -> {error, bad_base64}
	end;
decode_base64_with_size(_, _) ->
	{error, bad_type}.
