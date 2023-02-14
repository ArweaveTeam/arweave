-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-include_lib("eunit/include/eunit.hrl").

%% @doc The XXX_parse_test() tests assert that a correctly formatted p3 configuration block is
%% correctly parsed. They do *not* validate that the configuration data is semantically
%% correct - that level of business logic validation is asserted elsewhere.
empty_config_parse_test() ->
	Config = <<"{}">>,
	{ok, ParsedConfig} = ar_config:parse(Config),
	ExpectedConfig = [],
	?assertEqual(ExpectedConfig, ParsedConfig#config.services).

no_service_parse_test() ->
	Config = <<"{\"services\": []}">>,
	{ok, ParsedConfig} = ar_config:parse(Config),
	ExpectedConfig = [],
	?assertEqual(ExpectedConfig, ParsedConfig#config.services).

basic_parse_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		},
		{
			\"endpoint\": \"/chunk/{offset}\",
			\"modSeq\": 5,
			\"rates\": {
				\"rate_type\": \"byte\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"100000\",
						\"address\": \"def\"
					}
				}
			}
		}]
	}">>,
	{ok, ParsedConfig} = ar_config:parse(Config),
	ExpectedConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"abc">>
					}
				}
			}
		},
		#p3_service{
			endpoint = <<"/chunk/{offset}">>,
			mod_seq = 5,
			rates = #p3_rates{
				rate_type = <<"byte">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"100000">>,
						address = <<"def">>
					}
				}
			}
		}
	],
	?assertEqual(ExpectedConfig, ParsedConfig#config.services).

%% @doc The XXX_parse_error_test() tests assert that an incorrectly formatted p3 configuration
%% block triggers an error during config file load. They do consider semantic or business
%% logic errors.
no_service_list_parse_error_test() ->
	Config = <<"{
		\"services\": {
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		}
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).

bad_service_token_parse_error_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"invalid\": \"value\",
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		}]
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).

modseq_not_integer_parse_error_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": \"1\",
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		}]
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).

bad_rates_token_parse_error_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"rates\": {
				\"rate_type\": \"request\",
				\"invalid\": \"value\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		}]
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).

bad_arweave_token_parse_error_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"invalid\": \"value\",
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\"
					}
				}
			}
		}]
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).


bad_ar_token_parse_error_test() ->
	Config = <<"{
		\"services\": [{
			\"endpoint\": \"/info\",
			\"modSeq\": 1,
			\"rates\": {
				\"rate_type\": \"request\",
				\"arweave\": {
					\"AR\": {
						\"price\": \"1000\",
						\"address\": \"abc\",
						\"invalid\": \"value\"
					}
				}
			}
		}]
	}">>,
	?assertMatch(
		{error, {bad_format, services, _}, _}, ar_config:parse(Config)).