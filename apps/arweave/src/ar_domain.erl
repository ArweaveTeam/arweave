-module(ar_domain).

-export([get_labeling/3, lookup_arweave_txt_record/1, derive_tx_label/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

get_labeling(ApexDomain, CustomDomains, Hostname) ->
	Size = byte_size(ApexDomain),
	case binary:match(Hostname, ApexDomain) of
		{0, Size} ->
			apex;
		{N, Size} ->
			Label = binary:part(Hostname, {0, N-1}),
			{labeled, Label};
		nomatch ->
			get_labeling_1(CustomDomains, Hostname)
	end.

lookup_arweave_txt_record(Domain) ->
	case inet_res:lookup("_arweave." ++ binary_to_list(Domain), in, txt) of
		[] ->
			not_found;
		[RecordChunks|_] ->
			list_to_binary(lists:concat(RecordChunks))
	end.

derive_tx_label(TXID, BH) ->
	Data = <<TXID/binary, BH/binary>>,
	Digest = crypto:hash(sha256, Data),
	binary:part(ar_base32:encode(Digest), {0, 12}).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_labeling_1(CustomDomains, Hostname) ->
	case lists:member(Hostname, CustomDomains) of
		true ->
			{custom, Hostname};
		false ->
			unknown
	end.
