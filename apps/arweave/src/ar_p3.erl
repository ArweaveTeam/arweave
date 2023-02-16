-module(ar_p3).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-export([start_link/0, request/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([validate_config/1]).

request(Method, SplitPath, Req) ->
	gen_server:call(?MODULE, {request, Method, SplitPath, Req}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	
	{ok, Config} = application:get_env(arweave, config),
	validate_config(Config).


handle_call({request, Method, SplitPath, Req}, _From, State) ->
	{reply, {true, ok}, State}.

handle_cast(Message, State) ->
	NewState = State,
	{noreply, NewState}.

handle_info(Info, State) ->
	NewState = State,
	{noreply, NewState}.

terminate(Reason, State) ->
	ok.


validate_config(Config) ->
	case lists:all(fun validate_service/1, maps:to_list(Config#config.services)) of
		true ->
			{ok, Config#config.services};
		false ->
			{stop, "Error validating services"}
	end.

validate_service({Endpoint, ServiceConfig}) when is_record(ServiceConfig, p3_service) ->
	validate_endpoint(Endpoint) andalso
	validate_endpoint(ServiceConfig#p3_service.endpoint) andalso
	Endpoint == ServiceConfig#p3_service.endpoint andalso
	validate_mod_seq(ServiceConfig#p3_service.mod_seq) andalso
	validate_rates(ServiceConfig#p3_service.rates);

validate_service(_) ->
	false.

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

validate_rates(Rates) when is_record(Rates, p3_rates) ->
	lists:member(Rates#p3_rates.rate_type, ?P3_RATE_TYPES) andalso
	validate_arweave(Rates#p3_rates.arweave);

validate_rates(_) ->
	false.

validate_arweave(Arweave) when is_record(Arweave, p3_arweave) ->
	validate_ar(Arweave#p3_arweave.ar);

validate_arweave(_) ->
	false.

validate_ar(Ar) when is_record(Ar, p3_ar) ->
	validate_ar_price(Ar#p3_ar.price) andalso
	validate_ar_address(Ar#p3_ar.address);

validate_ar(_) ->
	false.

validate_ar_price(Price) ->
	try
		_ = binary_to_integer(Price),
		true
	catch error:badarg ->	
		false
	end.

validate_ar_address(Address) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(
			Address) of
		{error, invalid} ->
			false;
		{ok, _Addr} ->
			%% TODO: is there any more validation we can do here? length?
			true
	end.