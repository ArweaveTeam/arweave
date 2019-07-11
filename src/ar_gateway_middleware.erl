-module(ar_gateway_middleware).
-behaviour(cowboy_middleware).
-export([execute/2]).

-include("ar.hrl").
-define(BH_SEARCH_TIMEOUT, 25000).

%%%===================================================================
%%% Cowboy middleware callback.
%%%===================================================================

%% @doc Handle gateway requests.
%%
%% When gateway mode is turned on, a domain name needs to be provided
%% to act as the main domain name for this gateway. This domain name
%% is referred to as the apex domain for this gateway. A number of
%% custom domains that will map to specific transactions can also be
%% provided.
%%
%% The main purpose of a gateway is to serve different transaction on
%% different origins. This is done by deriving what will be referred
%% to as a transaction label from the transaction's ID and its block's
%% hash and prefixing that label to the apex domain of the gateway.
%% For example:
%%
%%     Accessing    https://gateway.example/{txid}
%%     redirects to https://{txlabel}.gateway.example/{txid}
%%     where it then serves the transaction's content.
%%
%% A gateway also allows to configure custom domains which will
%% redirect to specific transactions. For example, if
%% custom.domain.example is defined as a custom domain:
%%
%%     Accessing https://custom.domain.example
%%     triggers a TXT record lookup at _arweave.custom.domain.example
%%     where a transaction ID is expected to be found.
%%
%% This transaction ID is then used to decide from which transaction
%% to serve the content for this custom domain.
%%
%% The gateway also allows using relative links whether accessing a
%% transaction's content through a labeled domain name or through a
%% custom domain name. For example:
%%
%%     Accessing    https://{tx1label}.gateway.example/{tx2id}
%%     redirects to https://{tx2label}.gateway.example/{tx2id}
%%     where tx2's content is then served, and
%%
%%     Accessing    https://custom.domain.example/{txid}
%%     redirects to https://{txlabel}.gateway.example/{txid}
%%     where tx's content is then served.
%%
%% This allows webpages stored in transactions to link to each other
%% without having to refer to any specific gateway. That is to say, to
%% link to a different transaction in HTML, one would use
%%
%%     <a href="{txid}">Link</a>
%%
%% rather than
%%
%%     <a href="https://{txlabel}.gateway.example/{txid}">Link</a>
execute(Req, #{ gateway := {Domain, CustomDomains} } = Env) ->
	Hostname = cowboy_req:host(Req),
	case ar_domain:get_labeling(Domain, CustomDomains, Hostname) of
		apex -> handle_apex_request(Req, Env);
		{labeled, Label} -> handle_labeled_request(Label, Req, Env);
		{custom, CustomDomain} -> handle_custom_request(CustomDomain, Req, Env);
		unknown -> handle_unknown_request(Req, Env)
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

handle_apex_request(Req, Env) ->
	case resolve_path(cowboy_req:path(Req)) of
		{tx, TXID, _} -> derive_label_and_redirect(TXID, Req, Env);
		other -> other_request(Req, Env);
		root -> bad_request(Req);
		invalid -> bad_request(Req);
		not_found -> not_found(Req)
	end.

handle_labeled_request(Label, Req, Env) ->
	case resolve_path(cowboy_req:path(Req)) of
		{tx, TXID, Filename} ->
			handle_labeled_request_1(Label, TXID, Filename, Req, Env);
		other ->
			other_request(Req, Env);
		root ->
			bad_request(Req);
		invalid ->
			bad_request(Req);
		not_found ->
			not_found(Req)
	end.

handle_labeled_request_1(Label, TXID, Filename, Req, Env) ->
	case get_tx_block_hash(TXID) of
		{ok, BH} ->
			handle_labeled_request_2(Label, TXID, Filename, BH, Req, Env);
		not_found ->
			not_found(Req)
	end.

handle_labeled_request_2(Label, TXID, Filename, BH, Req, Env) ->
	case ar_domain:derive_tx_label(TXID, BH) of
		Label ->
			handle_labeled_request_3(Label, TXID, Filename, Req, Env);
		OtherLabel ->
			redirect_to_labeled_tx(OtherLabel, TXID, Req, Env)
	end.

handle_labeled_request_3(Label, TXID, Filename, Req, Env) ->
	case cowboy_req:scheme(Req) of
		<<"https">> -> serve_tx(Filename, Req);
		<<"http">> -> redirect_to_labeled_tx(Label, TXID, Req, Env)
	end.

handle_custom_request(CustomDomain, Req, Env) ->
	case resolve_path(cowboy_req:path(Req)) of
		root -> handle_custom_request_1(CustomDomain, Req);
		{tx, TXID, _} -> derive_label_and_redirect(TXID, Req, Env);
		other -> other_request(Req, Env);
		invalid -> bad_request(Req)
	end.

handle_unknown_request(Req, Env) ->
	case cowboy_req:scheme(Req) of
		<<"https">> -> not_found(Req);
		<<"http">> -> other_request(Req, Env)
	end.

resolve_path(Path) ->
	case ar_http_iface_server:split_path(Path) of
		[] -> root;
		[<<Hash:43/bytes>>] -> resolve_path_1(Hash);
		[_ | _] -> other
	end.

resolve_path_1(Hash) ->
	case get_tx_from_hash(Hash) of
		{ok, TXID, Filename} -> {tx, TXID, Filename};
		invalid -> other;
		not_found -> not_found
	end.

derive_label_and_redirect(TXID, Req, Env) ->
	case get_tx_block_hash(TXID) of
		{ok, BH} -> derive_label_and_redirect_1(TXID, BH, Req, Env);
		not_found -> not_found(Req)
	end.

get_tx_block_hash(TXID) ->
	HttpSearchNode = whereis(http_search_node),
	case ar_tx_search:get_tags_by_id(HttpSearchNode, TXID, ?BH_SEARCH_TIMEOUT) of
		{ok, PseudoTags} -> get_tx_block_hash_1(PseudoTags);
		{error, _} -> not_found
	end.

get_tx_block_hash_1(PseudoTags) ->
	case proplists:get_value(<<"block_indep_hash">>, PseudoTags) of
		undefined -> not_found;
		BH -> {ok, BH}
	end.

derive_label_and_redirect_1(TXID, BH, Req, Env) ->
	Label = ar_domain:derive_tx_label(TXID, BH),
	redirect_to_labeled_tx(Label, TXID, Req, Env).

redirect_to_labeled_tx(Label, TXID, Req, #{ gateway := {Domain, _} }) ->
	Hash = ar_util:encode(TXID),
	Location = <<
		"https://",
		Label/binary, ".",
		Domain/binary, "/",
		Hash/binary
	>>,
	{stop, cowboy_req:reply(301, #{<<"location">> => Location}, Req)}.

other_request(Req, Env) ->
	case cowboy_req:scheme(Req) of
		<<"https">> ->
			{ok, Req, Env};
		<<"http">> ->
			Hostname = cowboy_req:host(Req),
			Path = cowboy_req:path(Req),
			Location = <<
				"https://",
				Hostname/binary,
				Path/binary
			>>,
			{stop, cowboy_req:reply(301, #{<<"location">> => Location}, Req)}
	end.

bad_request(Req) ->
	{stop, cowboy_req:reply(400, Req)}.

not_found(Req) ->
	{stop, cowboy_req:reply(404, Req)}.

handle_custom_request_1(CustomDomain, Req) ->
	case cowboy_req:scheme(Req) of
		<<"https">> -> handle_custom_request_2(CustomDomain, Req);
		<<"http">> -> redirect_to_secure_custom_domain(CustomDomain, Req)
	end.

handle_custom_request_2(CustomDomain, Req) ->
	case ar_domain:lookup_arweave_txt_record(CustomDomain) of
		not_found ->
			domain_improperly_configured(Req);
		TxtRecord ->
			handle_custom_request_3(TxtRecord, Req)
	end.

handle_custom_request_3(TxtRecord, Req) ->
	case get_tx_from_hash(TxtRecord) of
		{ok, _, Filename} -> serve_tx(Filename, Req);
		invalid -> domain_improperly_configured(Req);
		not_found -> domain_improperly_configured(Req)
	end.

redirect_to_secure_custom_domain(CustomDomain, Req) ->
	Location = <<
		"https://",
		CustomDomain/binary, "/"
	>>,
	{stop, cowboy_req:reply(301, #{<<"location">> => Location}, Req)}.

get_tx_from_hash(Hash) ->
	case ar_util:safe_decode(Hash) of
		{ok, TXID} -> get_tx_from_hash_1(TXID);
		{error, _} -> invalid
	end.

get_tx_from_hash_1(TXID) ->
	case ar_storage:lookup_tx_filename(TXID) of
		unavailable -> not_found;
		Filename -> {ok, TXID, Filename}
	end.

serve_tx(Filename, Req) ->
	TX = ar_storage:read_tx_file(Filename),
	ContentType = proplists:get_value(<<"Content-Type">>, TX#tx.tags, "text/html"),
	Headers = #{<<"content-type">> => ContentType},
	{stop, cowboy_req:reply(200, Headers, TX#tx.data, Req)}.

domain_improperly_configured(Req) ->
	{stop, cowboy_req:reply(502, #{}, <<"Domain improperly configured">>, Req)}.
