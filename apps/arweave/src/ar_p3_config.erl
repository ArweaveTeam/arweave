-module(ar_p3_config).

-export([parse_services/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_p3.hrl").

%% -------------------------------------------------------------------
%% P3 Service Config JSON structure:
%%
%% "services": [
%%   {
%%     "endpoint":"/info",
%%     "modSeq": 1,
%%     "rates": {
%%       "rate_type": "request",
%%       "arweave": {
%%         "AR": {
%%           "price":"10000",
%%           "address":"89tR0-C1m3_sCWCoVCChg4gFYKdiH5_ZDyZpdJ2DDRw"
%%         },
%%         "VRT": {
%%           "price":"1",
%%           "contractId":"aLemOhg9OGovn-0o4cOCbueiHT9VgdYnpJpq7NgMA1A",
%%           "address":"89tR0-C1m3_sCWCoVCChg4gFYKdiH5_ZDyZpdJ2DDRw"
%%         }
%%       },
%%     }
%%   },
%%   {
%%     "endpoint":"/tx/{id}",
%%     ...
%%   }
%% ]
%% -------------------------------------------------------------------

%% @doc Parse a list of services:
%% "services": [ {service}, {service}, ... ]
parse_services(ServicesConfig) when is_list(ServicesConfig) ->
	Services = [parse_service(ServiceConfig, #p3_service{}) || {ServiceConfig} <- ServicesConfig],
	lists:foldl(
		fun(Service, Acc) -> 
			maps:put(Service#p3_service.endpoint, Service, Acc)
		end,
		#{},
		Services
	);

parse_services(ServicesConfig) ->
	erlang:error(
		"'services' object must be a list of 'service' objects.",
		ServicesConfig).

%% @doc Parse each token in a service object
%% {"endpoint": "/info", "modSeq": 1, "rates": {rates}}
parse_service([{<<"endpoint">>, Endpoint} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ endpoint = Endpoint });
	
parse_service([{<<"modSeq">>, ModSeq} | Rest], ServiceConfig) when is_integer(ModSeq) ->
	parse_service(Rest, ServiceConfig#p3_service{ mod_seq = ModSeq });

parse_service([{<<"rates">>, {Rates}} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ rates = parse_rates(Rates, #p3_rates{}) });

parse_service([], ServiceConfig) ->
	ServiceConfig;

parse_service(BadToken, _ServiceConfig) ->
	erlang:error(
		"Unexpected 'services' token. Valid tokens: 'endpoint', 'modSeq', 'rates'.",
		BadToken).

%% @doc Parse each token in the rates object
%% {"rate_type": "request", "arweave": {arweave}}
parse_rates([{<<"rate_type">>, RateType} | Rest], RatesConfig) ->
	parse_rates(Rest, RatesConfig#p3_rates{ rate_type = RateType });

parse_rates([{<<"arweave">>, {ArweaveRates}} | Rest], RatesConfig) ->
	parse_rates(
		Rest,
		RatesConfig#p3_rates{ arweave = parse_arweave(ArweaveRates, #p3_arweave{}) });

parse_rates([], RatesConfig) ->
	RatesConfig;

parse_rates(BadToken, _RatesConfig) ->
	erlang:error(
		"Unexpected 'rates' token. Valid tokens: 'rate_type', 'arweave'.",
		BadToken).

%% @doc Parse each currency in the arweave object
%% {"AR": {ar}, "VRT": {vrt}}
%% Note: Initially only AR is supported
parse_arweave([{<<"AR">>, {AR}} | Rest], ArweaveConfig) ->
	parse_arweave(Rest, ArweaveConfig#p3_arweave{ ar = parse_ar(AR, #p3_ar{}) });

parse_arweave([], ArweaveConfig) ->
	ArweaveConfig;

parse_arweave(BadToken, _ArweaveConfig) ->
	erlang:error(
		"Unexpected 'arweave' token. Valid tokens: 'AR'.",
		BadToken).

%% @doc Parse each token in the ar object
%% {"price": "1000", "address": "abc"}
parse_ar([{<<"price">>, Price} | Rest], ArConfig) ->
	parse_ar(Rest, ArConfig#p3_ar{ price = Price });

parse_ar([{<<"address">>, Address} | Rest], ArConfig) ->
	parse_ar(Rest, ArConfig#p3_ar{ address = Address });

parse_ar([], ArConfig) ->
	ArConfig;

parse_ar(BadToken, _ArConfig) ->
	erlang:error(
		"Unexpected 'ar' token. Valid tokens: 'price', 'address'.",
		BadToken).