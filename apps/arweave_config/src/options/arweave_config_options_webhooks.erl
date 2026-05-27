%%% @doc Specs for the `webhooks` option group. Options for
%%% delivering node events to external HTTP consumers.
%%%
%%% Each webhook is stored under `[webhooks, WebhookID, Attr]`,
%%% where `Attr` is one of `enabled`, `url`, `events`, `headers`.
%%% Webhooks created from legacy `config.json` get synthesized IDs
%%% (`legacy_1`, `legacy_2`, ...); operators pick their own IDs
%%% otherwise. The aggregate `arweave_config:webhooks/0' reconstructs
%%% the per-webhook entries into a list of
%%% `#{events, url, headers}' maps via
%%% `arweave_config:get_all_with_prefix/1'.
%%%
%%% The webhook validator rejects enabled webhooks that are missing
%%% a url or events list.
-module(arweave_config_options_webhooks).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	validate/0,
	list/0,
	write_legacy_list/1
]).
-include("arweave_config.hrl").

specs() ->
	[
		%-----------------------------------------------------
		% Per-webhook leaves
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [webhooks, {webhook_id}, enabled],
			default => true,
			type => boolean,
			short_description =>
				<<"Whether the webhook fires (true) or is inert "
				  "(false). Disabled hooks remain in the store but "
				  "are skipped by the legacy list view.">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {webhook_id}, url],
			default => undefined,
			short_description =>
				<<"Target URL the webhook posts to.">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {webhook_id}, events],
			default => [],
			short_description =>
				<<"List of event atoms the webhook subscribes to "
				  "(e.g. transaction, block).">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {webhook_id}, headers],
			default => [],
			short_description =>
				<<"HTTP headers to send with each webhook request, "
				  "as a list of {key, value} pairs.">>
		}
	].

group_description() ->
	<<"Manage webhook delivery behavior.">>.

%% @doc Take a legacy list of `#{events, url, headers}' maps,
%% synthesize IDs (`legacy_1', `legacy_2', …), and write each leaf
%% into the webhook_id store.
-spec write_legacy_list([map()]) -> ok.
write_legacy_list(Webhooks) when is_list(Webhooks) ->
	%% Clear any prior `legacy_*` webhook IDs so a re-load doesn't
	%% accumulate stale entries.
	clear_legacy_instances(),
	lists:foldl(
		fun(Hook, N) ->
			Id = legacy_id(N),
			write_one(Id, Hook),
			N + 1
		end,
		1,
		Webhooks
	),
	ok.

write_one(Id, Webhook) when is_map(Webhook) ->
	Set = fun(K, V) ->
		arweave_config_options_registry:set_local([webhooks, Id, K], V)
	end,
	_ = Set(enabled, true),
	_ = Set(url,     maps:get(url, Webhook, undefined)),
	_ = Set(events,  maps:get(events, Webhook, [])),
	_ = Set(headers, maps:get(headers, Webhook, [])),
	ok.

clear_legacy_instances() ->
	Items = arweave_config:get_all_with_prefix([webhooks]),
	[arweave_config_store:delete(Key)
		|| {Key, _Value} <- Items,
		   length(Key) >= 2,
		   is_legacy_id(lists:nth(2, Key))],
	ok.

is_legacy_id(Atom) when is_atom(Atom) ->
	%% atoms named `legacy_<N>` are bridge-synthesized.
	S = atom_to_list(Atom),
	case S of
		"legacy_" ++ Rest ->
			try _ = list_to_integer(Rest), true
			catch _:_ -> false end;
		_ ->
			false
	end;
is_legacy_id(_) ->
	false.

legacy_id(N) when is_integer(N), N > 0 ->
	list_to_atom("legacy_" ++ integer_to_list(N)).

%% @doc Aggregate per-webhook entries into a list of
%% `#{events, url, headers}' maps, sorted by webhook id. Disabled
%% webhooks are filtered out.
-spec list() -> [#{events => list(),
                   url => binary() | undefined,
                   headers => list()}].
list() ->
	ByWebhook = group_by_id(
		arweave_config:get_all_with_prefix([webhooks])),
	[#{
		events => maps:get(events, A, []),
		url => maps:get(url, A, undefined),
		headers => maps:get(headers, A, [])
	 }
	 || {_Id, A} <- lists:sort(maps:to_list(ByWebhook)),
	    maps:get(enabled, A, false) =:= true].

%% @doc Webhook validator. Every enabled webhook must
%% have a non-empty URL and non-empty events list. Disabled webhooks
%% are skipped.
-spec validate() -> ok | {error, binary()}.
validate() ->
	ByWebhook = group_by_id(
		arweave_config:get_all_with_prefix([webhooks])),
	validate_each(maps:to_list(ByWebhook)).

%% Group `[{[webhooks, Id, Attr], Value}]' into
%% `#{Id => #{Attr => Value}}'.
group_by_id(Entries) ->
	lists:foldl(
		fun({[webhooks, Id, Attr], V}, Acc) ->
			Existing = maps:get(Id, Acc, #{}),
			Acc#{Id => Existing#{Attr => V}};
		   (_, Acc) -> Acc
		end, #{}, Entries).

validate_each([]) ->
	ok;
validate_each([{Id, Attrs} | Rest]) ->
	case maps:get(enabled, Attrs, false) of
		false ->
			validate_each(Rest);
		true ->
			case validate_one(Id, Attrs) of
				ok -> validate_each(Rest);
				{error, _} = Err -> Err
			end
	end.

validate_one(Id, Attrs) ->
	Url = maps:get(url, Attrs, undefined),
	Events = maps:get(events, Attrs, []),
	case {Url, Events} of
		{undefined, _} ->
			{error, format_error(Id, <<"missing url">>)};
		{<<>>, _} ->
			{error, format_error(Id, <<"empty url">>)};
		{_, []} ->
			{error, format_error(Id, <<"events list is empty">>)};
		_ ->
			ok
	end.

format_error(Id, Reason) ->
	IDBin = if
		is_atom(Id) -> atom_to_binary(Id);
		is_binary(Id) -> Id;
		true -> list_to_binary(io_lib:format("~p", [Id]))
	end,
	<<"webhook ", IDBin/binary, ": ", Reason/binary>>.
