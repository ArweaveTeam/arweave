-module(ar_gateway_middleware_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/ar.hrl").

-define(MOCK_DOMAIN, <<"gateway.test">>).
-define(MOCK_CUSTOM_DOMAINS, []).
-define(MOCK_ENV, #{ gateway => {?MOCK_DOMAIN, ?MOCK_CUSTOM_DOMAINS} }).

-define(MOCK_TXID, <<
	84, 149, 29, 45, 202, 49, 52, 3, 105, 79, 42, 68, 153, 32, 130, 193,
	161, 80, 254, 205, 177, 34, 211, 210, 234, 32, 253, 228, 43, 175, 198, 13
>>).

-define(MOCK_BLOCKID, <<
	169, 43, 214, 105, 100, 19, 171, 187, 255, 185, 104, 174, 45, 225, 210, 86,
	75, 211, 187, 148, 156, 59, 69, 31, 77, 165, 56, 190, 20, 75, 14, 207,
	212, 134, 4, 201, 13, 244, 65, 73, 236, 174, 28, 174, 27, 250, 30, 246
>>).

-define(MOCK_TXLABEL, <<"ez63a7fvuu3l">>).

execute_test_() ->
	{foreach, fun setup/0, fun teardown/1, [
		{"redirect root to labeled domain", fun redirect_root_to_labeled_domain/0},
		{"serve correctly labeled request", fun serve_correctly_labeled_request/0}
	]}.

setup() ->
	meck:new(ar_storage),
	meck:new(ar_tx_search).

teardown(_) ->
	meck:unload().

redirect_root_to_labeled_domain() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> <<"some/mock/path">> end
	),
	meck:expect(
		ar_tx_search,
		get_tags_by_id,
		fun(?MOCK_TXID) ->
			{ok, [{<<"block_indep_hash">>, ?MOCK_BLOCKID}]}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{response, Status, Headers, _} =
		req(
			<<"https">>,
			?MOCK_DOMAIN,
			<<"/", Hash/binary>>
		),
	#{ <<"location">> := Location } = Headers,
	?assertEqual(<<
		"https://",
		?MOCK_TXLABEL/binary, ".",
		?MOCK_DOMAIN/binary, "/",
		Hash/binary
	>>, Location),
	?assertEqual(301, Status),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_tx_search)).

serve_correctly_labeled_request() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> <<"some/mock/path">> end
	),
	meck:expect(
		ar_tx_search,
		get_tags_by_id,
		fun(?MOCK_TXID) ->
			{ok, [{<<"block_indep_hash">>, ?MOCK_BLOCKID}]}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun(<<"some/mock/path">>) ->
			#tx {
				tags = [{<<"Content-Type">>, <<"text/plain">>}],
				data = <<"Some mock data">>
			}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{response, Status, Headers, Body} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary>>
		),
	?assertEqual(200, Status),
	#{ <<"content-type">> := ContentType } = Headers,
	?assertEqual(<<"text/plain">>, ContentType),
	?assertEqual(<<"Some mock data">>, Body),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_tx_search)).

req(Scheme, Host, Path) ->
	Pid = self(),
	StreamId = make_ref(),
	Req = #{
		scheme => Scheme,
		host => Host,
		path => Path,
		pid => self(),
		streamid => StreamId
	},
	ar_gateway_middleware:execute(Req, ?MOCK_ENV),
	receive
		{{Pid, StreamId}, V} -> V
	end.
