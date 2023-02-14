-module(ar_p3_config).

-export([parse_services/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").


parse_services(ServicesConfig) when is_list(ServicesConfig) ->
	[parse_service(Service) || {Service} <- ServicesConfig].

parse_service([{<<"endpoint">>, Endpoint} | Rest]) ->
	maps:put(endpoint, Endpoint, parse_service(Rest));
	
parse_service([{<<"modSeq">>, ModSeq} | Rest]) when is_integer(ModSeq) ->
	maps:put(mod_seq, ModSeq, parse_service(Rest));

parse_service([{<<"rates">>, {Rates}} | Rest]) ->
	maps:put(rates, parse_rates(Rates), parse_service(Rest));

parse_service([]) ->
	#{};

parse_service({A}) ->
	?LOG_ERROR("parse_service A: ~p", [A]),
	ok.

parse_rates([{<<"rate_type">>, RateType} | Rest]) ->
	maps:put(rate_type, RateType, parse_rates(Rest));

parse_rates([{<<"arweave">>, ArweaveRates} | Rest]) ->
	maps:put(arweave, parse_arweave(ArweaveRates), parse_rates(Rest)).

parse_arweave(ArweaveRates) ->
	[parse_arweave_token(Token) || {Token} <- ArweaveRates].

parse_arweave_token([{<<"AR">>, {AR}} | Rest]) ->
	maps:put(ar, parse_ar(AR), parse_arweave_token(Rest));

parse_arweave_token([]) ->
	#{}.

parse_ar([{<<"price">>, Price} | Rest]) ->
	maps:put(price, Price, parse_ar(Rest));

parse_ar([{<<"address">>, Address} | Rest]) ->
	maps:put(address, Address, parse_ar(Rest));

parse_ar([]) ->
	#{}.