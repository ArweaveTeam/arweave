%%% @doc Specs for the `vdf` option group. Options for
%%% configuring how the node produces and validates VDF outputs.
-module(arweave_config_options_vdf).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([set_algorithm/4, set_compute/4]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [vdf, algorithm],
			default => openssl,
			legacy => vdf,
			short_description =>
				<<"VDF implementation: openssl, openssllite, fused, "
				  "or hiopt_m4.">>,
			handle_set => fun arweave_config_options_vdf:set_algorithm/4
		},
		#{
			enabled => true,
			option_key => [vdf, compute],
			default => auto,
			legacy => vdf_compute,
			short_description =>
				<<"Whether the node computes its own VDF.">>,
			long_description =>
				<<"`auto` decides from the configured VDF server "
				  "peers — true if no `vdf_server` peer is "
				  "configured, false otherwise. Override with "
				  "`true` or `false` explicitly.">>,
			handle_set => fun arweave_config_options_vdf:set_compute/4
		},
		#{
			enabled => true,
			option_key => [vdf, is_public_server],
			default => false,
			type => boolean,
			legacy => vdf_is_public_server,
			short_description =>
				<<"Expose the local VDF server publicly (no client "
				  "allowlist).">>
		},
		#{
			enabled => true,
			option_key => [vdf, pull],
			default => true,
			type => boolean,
			legacy => vdf_pull,
			short_description =>
				<<"Pull VDF values from the configured remote VDF "
				  "server.">>
		},
		#{
			enabled => true,
			option_key => [vdf, max_validation_threads],
			default => ?DEFAULT_MAX_NONCE_LIMITER_VALIDATION_THREAD_COUNT,
			type => pos_integer,
			legacy => max_nonce_limiter_validation_thread_count,
			short_description =>
				<<"Maximum number of threads used for VDF "
				  "validation.">>
		},
		#{
			enabled => true,
			option_key => [vdf, max_last_step_validation_threads],
			default =>
				?DEFAULT_MAX_NONCE_LIMITER_LAST_STEP_VALIDATION_THREAD_COUNT,
			type => pos_integer,
			legacy => max_nonce_limiter_last_step_validation_thread_count,
			short_description =>
				<<"Maximum number of threads used for VDF last-step "
				  "validation.">>
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Control VDF computation, validation, and serving behavior.">>.

%%%===================================================================
%%% Transform helpers for the `handle_set/4` specs above. Each helper
%%% normalises atom and binary shapes to the canonical atom before
%%% returning `{store, Decoded}`.
%%%===================================================================

set_algorithm(_K, V, _S, _A) ->
	case decode_algorithm(V) of
		{ok, Decoded} -> {store, Decoded};
		{error, _} = Err -> Err
	end.

decode_algorithm(V) when V =:= openssl;
                        V =:= openssllite;
                        V =:= fused;
                        V =:= hiopt_m4 ->
	{ok, V};
decode_algorithm(<<"openssl">>) -> {ok, openssl};
decode_algorithm(<<"openssllite">>) -> {ok, openssllite};
decode_algorithm(<<"fused">>) -> {ok, fused};
decode_algorithm(<<"hiopt_m4">>) -> {ok, hiopt_m4};
decode_algorithm(V) -> {error, {bad_vdf_algorithm, V}}.

set_compute(_K, V, _S, _A) ->
	case decode_compute(V) of
		{ok, Decoded} -> {store, Decoded};
		{error, _} = Err -> Err
	end.

decode_compute(V) when V =:= auto; V =:= true; V =:= false -> {ok, V};
decode_compute(<<"auto">>) -> {ok, auto};
decode_compute(<<"true">>) -> {ok, true};
decode_compute(<<"false">>) -> {ok, false};
decode_compute(V) -> {error, {bad_vdf_compute, V}}.
