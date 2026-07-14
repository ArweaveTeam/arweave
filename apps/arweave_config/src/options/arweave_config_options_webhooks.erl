%%% @doc Specs for the `webhooks` option group. Options for
%%% delivering node events to external HTTP consumers.
%%%
%%% Webhooks are canonically stored as `[webhooks]`, a list of maps.
%%% Leaf specs use `{list_item}` to declare the fields available
%%% inside each list element: `enabled`, `url`, `events`, and
%%% `headers`. `list/0` returns the legacy webhook shape expected by
%%% existing node code.
%%%
%%% The webhook validator rejects enabled webhooks that are missing
%%% a url or events list, or that name an event `ar_webhook' cannot
%%% subscribe to.
-module(arweave_config_options_webhooks).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	validate/0,
	legacy_list/0,
	write_legacy_webhook/2,
	write_legacy_list/1
]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [webhooks],
			type => list_map,
			default => [],
			short_description =>
				<<"Webhook declarations.">>
		},
		%-----------------------------------------------------
		% Per-webhook leaves
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [webhooks, {list_item}, enabled],
			default => true,
			type => boolean,
			short_description =>
				<<"Whether the webhook fires (true) or is inert "
				  "(false). Disabled hooks remain in the store but "
				  "are skipped by the legacy list view.">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {list_item}, url],
			default => undefined,
			short_description =>
				<<"Target URL the webhook posts to.">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {list_item}, events],
			default => [],
			type => list,
			short_description =>
				<<"List of events the webhook subscribes to "
				  "(e.g. transaction, block).">>
		},
		#{
			enabled => true,
			option_key => [webhooks, {list_item}, headers],
			default => [],
			short_description =>
				<<"HTTP headers to send with each webhook request, "
				  "as a list of {key, value} pairs.">>
		}
	].

group_description() ->
	<<"Manage webhook delivery behavior.">>.

%% @doc Take a legacy list of `#{events, url, headers}' maps and
%% write the canonical `[webhooks]' list.
-spec write_legacy_list([map()]) -> ok.
write_legacy_list(Webhooks) when is_list(Webhooks) ->
	write_webhooks([normalize_webhook(Hook) || Hook <- Webhooks]),
	ok.

write_legacy_webhook(Id, Webhook) when is_map(Webhook) ->
	_ = Id,
	write_webhooks(list_all() ++ [normalize_webhook(Webhook)]),
	ok.

write_webhooks(Webhooks) ->
	_ = arweave_config_store:delete_prefix([webhooks]),
	_ = arweave_config_options_registry:set_local([webhooks], Webhooks),
	ok.

normalize_webhook(Webhook) ->
	#{
		enabled => maps:get(enabled, Webhook, true),
		events => maps:get(events, Webhook, []),
		url => maps:get(url, Webhook, undefined),
		headers => maps:get(headers, Webhook, [])
	}.

%% @doc Convert per-webhook entries into a list of
%% `#{events, url, headers}' maps, sorted by webhook id. Disabled
%% webhooks are filtered out.
-spec legacy_list() -> [#{events => [binary()],
                   url => binary() | undefined,
                   headers => list()}].
legacy_list() ->
	[
		maps:without([enabled], Hook)
		|| Hook <- list_all(),
		   maps:get(enabled, Hook, true) =:= true
	].

list_all() ->
	case arweave_config_store:get([webhooks]) of
		{ok, Webhooks} when is_list(Webhooks) ->
			[normalize_webhook(Hook) || Hook <- Webhooks];
		_ ->
			ByWebhook = group_by_id(
				arweave_config:get_all_with_prefix([webhooks])),
			[
				normalize_webhook(A)
				|| {_Id, A} <- lists:sort(maps:to_list(ByWebhook))
			]
	end.

%% @doc Webhook validator. Every enabled webhook must
%% have a non-empty URL and non-empty events list. Disabled webhooks
%% are skipped.
-spec validate() -> ok | {error, binary()}.
validate() ->
	validate_each(list_all()).

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
validate_each([Attrs | Rest]) ->
	case maps:get(enabled, Attrs, false) of
		false ->
			validate_each(Rest);
		true ->
			case validate_one(Attrs) of
				ok -> validate_each(Rest);
				{error, _} = Err -> Err
			end
	end.

validate_one(Attrs) ->
	Url = maps:get(url, Attrs, undefined),
	Events = maps:get(events, Attrs, []),
	case {Url, Events} of
		{undefined, _} ->
			{error, <<"webhook: missing url">>};
		{<<>>, _} ->
			{error, <<"webhook: empty url">>};
		{_, []} ->
			{error, <<"webhook: events list is empty">>};
		_ ->
			validate_events(Events)
	end.

%% An unknown event name would otherwise leave a webhook that subscribes
%% to nothing and never fires. `ar_webhook' owns the vocabulary, so a new
%% event needs no change here.
validate_events(Events) ->
	Supported = ar_webhook:supported_events(),
	case [Event || Event <- Events, not lists:member(Event, Supported)] of
		[] ->
			ok;
		Unknown ->
			{error, iolist_to_binary(
				io_lib:format("webhook: unknown events ~p", [Unknown]))}
	end.
