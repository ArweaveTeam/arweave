-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

basic_test() ->
	?debugMsg("HELLO"),
	% gen_server:call(ar_p3, {one, two, three}),
	{ok, ParsedConfig} = ar_config:parse(get_info()),
	?debugFmt("FILE: ~p", [?REC_INFO(config, ParsedConfig)]).


empty_config() ->
	<<"{}">>.

no_services() ->
	<<"{
		\"services\": []
	}">>.

get_info() ->
	<<"{
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
		}]
	}">>.