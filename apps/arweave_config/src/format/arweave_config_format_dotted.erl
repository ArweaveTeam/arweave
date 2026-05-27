%%% @doc Walk a parsed JSON/YAML tree and produce a flat per-leaf
%%% config map suitable for `arweave_config:load/1'.
%%%
%%% Rules:
%%%
%%%   - Top-level keys may be fully-qualified dotted paths
%%%     (e.g. `"mining.enabled"'). They're split via
%%%     `arweave_config_parser:key/1', which knows about bracketed
%%%     segments (`peers.[1.2.3.4:1984].trusted'). A top-level dotted
%%%     key whose value is a map (or a list of maps) is rejected —
%%%     dotted form requires a scalar (or list-of-scalars) value.
%%%   - Nested keys (at any depth below the root) are always literal
%%%     segments. Dots in nested keys are passed through verbatim,
%%%     which lets the registry's template matching pick up peer IDs
%%%     and similar dotted literals.
%%%   - Lists of maps fan out to positional integer-keyed paths
%%%     (e.g. `storage_modules: [{partition: 0}, …]' becomes
%%%     `[storage_modules, 1, partition] => 0').
%%%   - Two entries that resolve to the same path and value are
%%%     idempotent. Two entries that resolve to the same path with
%%%     different values produce `conflicting_config_key'.
%%%
%%% Per-leaf insertion (path resolution past the top level, value
%%% flattening, conflict detection) lives in `arweave_config_leaf_map'.
%%% This module only owns the dotted-key parsing concern.
-module(arweave_config_format_dotted).
-compile(warnings_as_errors).
-export([to_leaf_map/1]).

-spec to_leaf_map(Map) -> Return when
	Map :: map(),
	Return :: {ok, map()} | {error, map()}.
to_leaf_map(Map) when is_map(Map) ->
	walk_top(maps:to_list(Map), #{});
to_leaf_map(_) ->
	{error, #{reason => badarg}}.

walk_top([], LeafMap) ->
	{ok, LeafMap};
walk_top([{Key, Value} | Rest], LeafMap) ->
	case top_path(Key, Value) of
		{ok, Path} ->
			case arweave_config_leaf_map:set(Path, Value, LeafMap) of
				{ok, NewLeafMap} -> walk_top(Rest, NewLeafMap);
				{error, _} = Err -> Err
			end;
		{error, _} = Err ->
			Err
	end.

%% A top-level key may be dotted; if so it must be fully qualified
%% and its value must be a scalar (or list of scalars).
top_path(Key, Value) ->
	Bin = arweave_config_parser:format_segment(Key),
	case binary:match(Bin, <<".">>) of
		nomatch -> {ok, [arweave_config_leaf_map:convert_key(Key)]};
		_ -> dotted_path(Bin, Value)
	end.

dotted_path(Bin, Value) when is_map(Value) ->
	nested_value_error(Bin, Value);
dotted_path(Bin, Value) when is_list(Value) ->
	case lists:any(fun is_map/1, Value) of
		true -> nested_value_error(Bin, Value);
		false -> split(Bin)
	end;
dotted_path(Bin, _Value) ->
	split(Bin).

split(Bin) ->
	case arweave_config_parser:key(Bin) of
		{ok, Path} ->
			{ok, Path};
		{error, Reason} ->
			{error, #{
				reason => invalid_dotted_config_key,
				key => Bin,
				details => Reason
			}}
	end.

nested_value_error(Bin, Value) ->
	{error, #{
		reason => dotted_config_key_has_nested_value,
		key => Bin,
		value => Value
	}}.

