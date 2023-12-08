-module(ar_p3_config).

-export([
	parse_p3/2, validate_config/1, get_payments_value/3, get_service_config/2, get_rate/2,
	get_json/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-define(RATE_TYPE_MAP, #{ <<"request">> => <<"Price per request">> }).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% -------------------------------------------------------------------
%% P3 Service Config JSON structure:
%%
%% "p3": {
%%   "payments": {
%%     "arweave/AR": {
%%       "address": "BHAWuomQUIL18WON2LjqjDF4YuRDcmhme7wvFW2BDiU",
%%       "minimum_balance": "-1000000",
%%       "confirmations": 2
%%     }
%%   },
%%   "services": [
%%     {
%%       "endpoint":"/info",
%%       "modSeq": 1,
%%       "rate_type": "request",
%%       "rates": {
%%         "arweave/AR: {
%%           "price":"10000",
%%         },
%%         "arweave/VRT": {
%%           "price":"1",
%%         }
%%       }
%%     },
%%     {
%%       "endpoint":"/tx/{id}",
%%       ...
%%     }
%%   ]
%% }
%% -------------------------------------------------------------------

parse_p3([{<<"payments">>, {PaymentsConfig}} | Rest], P3Config) ->
	parse_p3(Rest, P3Config#p3_config{ payments = parse_payments(PaymentsConfig, #{}) });
parse_p3([{<<"services">>, ServicesConfig} | Rest], P3Config) ->
	parse_p3(Rest, P3Config#p3_config{ services = parse_services(ServicesConfig) });
parse_p3([], P3Config) ->
	P3Config;
parse_p3(BadToken, _P3Config) ->
	erlang:error(
		"Unexpected 'p3' token. Valid tokens: 'payments', 'services'.",
		BadToken).

validate_config(Config) when
	is_record(Config, config) andalso
	is_record(Config#config.p3, p3_config) ->
	PaymentsValid = validate_payments(Config#config.p3#p3_config.payments),
	ServicesValid = validate_services(Config#config.p3#p3_config.services),
	case PaymentsValid and ServicesValid of
		true ->
			{ok, Config#config.p3};
		false ->
			{stop, "Error validating P3 config"}
	end.

get_payments_value(P3Config, Asset, Field) when
		is_record(P3Config, p3_config) ->
	case maps:get(Asset, P3Config#p3_config.payments, undefined) of
		undefined ->
			undefined;
		PaymentConfig ->
			element(Field, PaymentConfig)
	end.

get_service_config(P3Config, Req) ->
	Path = ar_http_iface_server:label_req(Req),
	case Path of
		undefined ->
			undefined;
		_ ->
			maps:get(list_to_binary(Path), P3Config#p3_config.services, undefined)
	end.

get_rate(ServiceConfig, Asset) when is_record(ServiceConfig, p3_service) ->
	maps:get(Asset, ServiceConfig#p3_service.rates, undefined).

get_json(P3Config) when
		is_record(P3Config, p3_config) ->
	Map = #{
		<<"payment_methods">> => to_json_payments(P3Config#p3_config.payments),
		<<"endpoints">> => to_json_services(P3Config#p3_config.services, P3Config)
	},
	jiffy:encode(Map).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% -------------------------------------------------------------------
%% Parse JSON into the p3_config record
%% -------------------------------------------------------------------
parse_payments([{?ARWEAVE_AR, {Payment}} | Rest], PaymentsConfig) ->
	parse_payments(
		Rest,
		PaymentsConfig#{ ?ARWEAVE_AR => parse_payment(Payment, #p3_payment{}) });
parse_payments([], PaymentsConfig) ->
	PaymentsConfig;
parse_payments(BadToken, _PaymentsConfig) ->
	erlang:error(
		"'payments' object must be a map of assets to 'payment' objects. " ++
		"Currently only 'arweave/AR' is supported.",
		BadToken).

parse_payment([{?P3_ADDRESS_HEADER, Address} | Rest], PaymentConfig) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Address) of
		{error, invalid} ->
			erlang:error(
				"Invalid 'address' value. " ++
				"Must be a valid base64 encoded address with optional checksum.",
				Address);
		{ok, DecodedAddress} ->
			parse_payment(Rest, PaymentConfig#p3_payment{ address = DecodedAddress })
	end;

parse_payment([{<<"minimum_balance">>, MinimumBalance} | Rest], PaymentConfig) ->
	parse_payment(
		Rest,
		PaymentConfig#p3_payment{ minimum_balance = to_integer(MinimumBalance) });

parse_payment([{<<"confirmations">>, Confirmations} | Rest], PaymentConfig) ->
	parse_payment(
		Rest,
		PaymentConfig#p3_payment{ confirmations = to_integer(Confirmations) });

parse_payment([], PaymentConfig) ->
	PaymentConfig;

parse_payment(BadToken, _PaymentConfig) ->
	erlang:error(
		"Unexpected 'payment' token. " ++
		"Valid tokens: 'address', 'minimum_balance', 'confirmations'.",
		BadToken).

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
parse_service([{?P3_ENDPOINT_HEADER, Endpoint} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ endpoint = Endpoint });

parse_service([{<<"rate_type">>, RateType} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ rate_type = RateType });

parse_service([{<<"mod_seq">>, ModSeq} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ mod_seq = to_integer(ModSeq) });

parse_service([{<<"rates">>, {Rates}} | Rest], ServiceConfig) ->
	parse_service(Rest, ServiceConfig#p3_service{ rates = parse_rates(Rates, #{}) });

parse_service([], ServiceConfig) ->
	ServiceConfig;

parse_service(BadToken, _ServiceConfig) ->
	erlang:error(
		"Unexpected 'services' token. Valid tokens: " ++
		"'endpoint', 'modSeq', 'rate_type, 'rates'.",
		BadToken).

%% @doc Parse each token in the rates object
%% {"rate_type": "request", "arweave": {arweave}}
parse_rates([{?ARWEAVE_AR, Price} | Rest], RatesConfig) ->
	parse_rates(
		Rest,
		RatesConfig#{ ?ARWEAVE_AR => to_integer(Price) });

parse_rates([], RatesConfig) ->
	RatesConfig;

parse_rates(BadToken, _RatesConfig) ->
	erlang:error(
		"Unexpected 'rates' token. Only 'arweave/AR' is currenty supported.",
		BadToken).

%% -------------------------------------------------------------------
%% Convert the #p3_config record into a /rates JSON response
%% The response structure differs slightly from the config structure:
%% {
%%   "payment_methods": {
%%     "arweave": {
%%       "AR": {
%%         "address": "89tR0-C1m3_sCWCoVCChg4gFYKdiH5_ZDyZpdJ2DDRw",
%%         "minimum_balance": -1000,
%% 	       "confirmations": 2
%%       }
%%     }
%%   },
%%   "endpoints": [
%%     {
%%       "endpoint": "/info",
%%       "modSeq": 1,
%%       "rates": {
%%         "description": "Price per request",
%%         "arweave": {
%%           "AR" {
%%             "price": 1000,
%%             "address": "89tR0-C1m3_sCWCoVCChg4gFYKdiH5_ZDyZpdJ2DDRw"
%%           }
%%         }
%%       }
%%     }
%%   ]
%% }
%% -------------------------------------------------------------------

to_json_payments(PaymentsConfig) ->
	maps:fold(
		fun(Asset, PaymentConfig, Acc) ->
			{Network, Token} = ?FROM_P3_ASSET(Asset),
			Acc#{ Network => #{ Token => to_json_payment(PaymentConfig) } }
		end,
		#{},
		PaymentsConfig
	).

to_json_payment(PaymentConfig) ->
	#{
		<<"address">> => ar_util:encode(PaymentConfig#p3_payment.address),
		<<"minimum_balance">> => PaymentConfig#p3_payment.minimum_balance,
		<<"confirmations">> => PaymentConfig#p3_payment.confirmations
	}.

to_json_services(ServicesConfig, P3Config) ->
	maps:fold(
		fun(_Endpoint, ServiceConfig, Acc) ->
			[ to_json_service(ServiceConfig, P3Config) | Acc]
		end,
		[],
		ServicesConfig
	).

to_json_service(ServiceConfig, P3Config) ->
	Rates = to_json_rates(ServiceConfig#p3_service.rates, P3Config),
	#{
		<<"endpoint">> => ServiceConfig#p3_service.endpoint,
		<<"modSeq">> => ServiceConfig#p3_service.mod_seq,
		<<"rates">> => Rates#{
			<<"description">> => maps:get(ServiceConfig#p3_service.rate_type, ?RATE_TYPE_MAP)
		}
	}.

to_json_rates(RatesConfig, P3Config) ->
	maps:fold(
		fun(Asset, Price, Acc) ->
			{Network, Token} = ?FROM_P3_ASSET(Asset),
			Address = ar_util:encode(get_payments_value(P3Config, Asset, #p3_payment.address)),
			Acc#{
				Network => #{
					Token => #{
						<<"price">> => Price,
						<<"address">> => Address
					}
				}
			}
		end,
		#{},
		RatesConfig
	).

%% -------------------------------------------------------------------
%% Validate that the specified P3 config is valid
%% -------------------------------------------------------------------
validate_payments(PaymentsConfig) ->
	is_map(PaymentsConfig) andalso
	lists:all(fun validate_payment/1, maps:to_list(PaymentsConfig)).

validate_payment({Asset, PaymentConfig}) ->
	Asset == ?ARWEAVE_AR andalso
	is_record(PaymentConfig, p3_payment) andalso
	validate_address(PaymentConfig#p3_payment.address) andalso
	validate_minimum_balance(PaymentConfig#p3_payment.minimum_balance) andalso
	validate_confirmations(PaymentConfig#p3_payment.confirmations).

validate_address(Address) ->
	Address /= undefined.

validate_minimum_balance(MinimumBalance) ->
	is_integer(MinimumBalance).

validate_confirmations(Confirmations) ->
	is_integer(Confirmations) andalso Confirmations >= 0.

validate_services(ServicesConfig) ->
	is_map(ServicesConfig) andalso
	lists:all(fun validate_service/1, maps:to_list(ServicesConfig)).

validate_service({Endpoint, ServiceConfig})  ->
	is_record(ServiceConfig, p3_service) andalso
	validate_endpoint(Endpoint) andalso
	validate_endpoint(ServiceConfig#p3_service.endpoint) andalso
	Endpoint == ServiceConfig#p3_service.endpoint andalso
	validate_mod_seq(ServiceConfig#p3_service.mod_seq) andalso
	validate_rate_type(ServiceConfig#p3_service.rate_type) andalso
	validate_rates(ServiceConfig#p3_service.rates).

validate_endpoint(undefined) ->
	false;
validate_endpoint(Endpoint) ->
	EndpointString = binary_to_list(Endpoint),
	case ar_http_iface_server:label_http_path(Endpoint) of
		undefined ->
			false;
		Label when Label == EndpointString ->
			true;
		Label when Label /= EndpointString ->
			io:format(
				"Endpoint ~p is not a valid P3 service. Closest valid match: ~p",
				[EndpointString, Label]),
			false
	end.

validate_mod_seq(ModSeq) ->
	is_integer(ModSeq).

validate_rate_type(RateType) ->
	lists:member(RateType, ?P3_RATE_TYPES).

validate_rates(RatesConfig) when map_size(RatesConfig) == 0 ->
	false;
validate_rates(RatesConfig) ->
	is_map(RatesConfig) andalso
	lists:all(fun validate_rate/1, maps:to_list(RatesConfig)).

validate_rate({Asset, Price}) ->
	Asset == ?ARWEAVE_AR andalso
	is_integer(Price).

to_integer(Value) when is_integer(Value) ->
	Value;
to_integer(Value) when is_binary(Value) ->
	to_integer(binary_to_list(Value));
to_integer(Value) when is_list(Value) ->
	list_to_integer(Value).