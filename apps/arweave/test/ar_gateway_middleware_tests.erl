-module(ar_gateway_middleware_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/ar.hrl").

-define(MOCK_DOMAIN, <<"gateway.test">>).
-define(MOCK_CUSTOM_DOMAIN, <<"custom.domain.test">>).
-define(MOCK_ENV, #{ gateway => {?MOCK_DOMAIN, [?MOCK_CUSTOM_DOMAIN]} }).

-define(MOCK_TXID, <<
	84, 149, 29, 45, 202, 49, 52, 3, 105, 79, 42, 68, 153, 32, 130, 193,
	161, 80, 254, 205, 177, 34, 211, 210, 234, 32, 253, 228, 43, 175, 198, 13
>>).

-define(MOCK_TXHASH, <<"VJUdLcoxNANpTypEmSCCwaFQ_s2xItPS6iD95Cuvxg0">>).

-define(MOCK_BLOCKHASH, <<"qSvWaWQTq7v_uWiuLeHSVkvTu5ScO0UfTaU4vhRLDs_UhgTJDfRBSeyuHK4b-h72">>).

-define(MOCK_TXLABEL, <<"ez63a7fvuu3l">>).

-define(MANIFEST_INDEX_TXID, <<
	92, 107, 206, 33, 17, 203, 162, 58, 142, 193, 82, 199, 205, 201, 190, 14,
	54, 5, 77, 26, 132, 131, 130, 249, 240, 18, 210, 37, 237, 159, 110, 108
>>).

-define(MANIFEST_DOG_TXID, <<
	158, 115, 11, 58, 243, 223, 99, 147, 125, 173, 58, 194, 61, 208, 0, 46,
	32, 73, 227, 78, 1, 176, 210, 161, 173, 22, 237, 195, 181, 19, 68, 151
>>).

execute_test_() ->
	{foreach, fun setup/0, fun teardown/1, [
		{"redirect root to labeled domain", fun redirect_root_to_labeled_domain/0},
		{"serve correctly labeled request", fun serve_correctly_labeled_request/0},
		{"forward other requests to node api", fun forward_other_requests_to_node_api/0},
		{"render manifest listing", fun render_manifest_listing/0},
		{"redirect manifest to index", fun redirect_manifest_to_index/0},
		{"serve manifest subpath", fun serve_manifest_subpath/0},
		{"serve manifest subpath v2", fun serve_manifest_subpath_v2/0},
		{"handle multi-segment subpaths", fun handle_multi_segment_subpaths/0},
		{"redirect badly labeled manifest subpaths", fun redirect_badly_labeled_manifest_subpaths/0},
		{"return 421 on bad manifest", fun return_421_on_bad_manifest/0},
		{"return 404 on non-existent subpath", fun return_404_on_non_existent_subpath/0},
		{"forward on subpath of non-manifest", fun forward_on_subpath_of_non_manifest/0},
		{"serve manifest subpath on custom domain", fun serve_manifest_subpath_on_custom_domain/0},
		{"forward non-existent subpaths to node api", fun forward_non_existent_subpaths_to_node_api/0}
	]}.

setup() ->
	meck:new(ar_storage, [passthrough]),
	meck:new(ar_arql_db),
	meck:new(ar_domain),
	meck:expect(
		ar_domain,
		get_labeling,
		fun(ApexDomain, CustomDomains, Hostname) ->
			meck:passthrough([ApexDomain, CustomDomains, Hostname])
		end
	),
	meck:expect(
		ar_domain,
		derive_tx_label,
		fun(TXID, BH) ->
			meck:passthrough([TXID, BH])
		end
	).

teardown(_) ->
	meck:unload().

redirect_root_to_labeled_domain() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {ok, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, _} =
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
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

serve_correctly_labeled_request() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {migrated_v1, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun(<<"some/mock/path">>) ->
			{ok, #tx {
				tags = [{<<"Content-Type">>, <<"text/plain">>}],
				data = <<"Some mock data">>
			}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, Body} =
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
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

forward_other_requests_to_node_api() ->
	?assertEqual(forward, req(
		<<"https">>,
		?MOCK_DOMAIN,
		<<"/other/path">>
	)).

render_manifest_listing() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {migrated_v1, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun(<<"some/mock/path">>) ->
			{ok, #tx {
				tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
				data = listing_manifest_fixture()
			}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, Body} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary>>
		),
	?assertEqual(200, Status),
	?assertMatch(#{ <<"content-type">> := <<"text/html">> }, Headers),
	?assertMatch({_, _}, binary:match(Body, <<"<a href=\"img/dog.jpg\">img/dog.jpg</a>">>)),
	?assertMatch({_, _}, binary:match(Body, <<"<a href=\"&lt;script&gt;alert(&apos;XSS!&apos;);&lt;/script&gt;\">&lt;script&gt;alert(&apos;XSS!&apos;);&lt;/script&gt;</a>">>)),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

redirect_manifest_to_index() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {ok, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun(<<"some/mock/path">>) ->
			{ok, #tx {
				tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
				data = index_manifest_fixture()
			}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, _} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary>>
		),
	ExpectedLocation = <<
		"https://",
		?MOCK_TXLABEL/binary, ".",
		?MOCK_DOMAIN/binary, "/",
		Hash/binary, "/index.html"
	>>,
	#{ <<"location">> := ActualLocation } = Headers,
	?assertEqual(301, Status),
	?assertEqual(ExpectedLocation, ActualLocation),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

serve_manifest_subpath() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {migrated_v1, <<"path/to/manifest">>};
			(?MANIFEST_INDEX_TXID) -> {ok, <<"path/to/index">>}
		end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = index_manifest_fixture()
				}}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun
			(<<"path/to/index">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"text/html">>}],
					data = <<"<html><body>Some HTML</body></html>">>
				}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, Body} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/index.html">>
		),
	?assertEqual(200, Status),
	?assertMatch(#{ <<"content-type">> := <<"text/html">> }, Headers),
	?assertEqual(<<"<html><body>Some HTML</body></html>">>, Body),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

serve_manifest_subpath_v2() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {ok, <<"path/to/manifest">>};
			(?MANIFEST_INDEX_TXID) -> {migrated_v1, <<"path/to/index">>}
		end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					id = ?MOCK_TXID,
					format = 2,
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}]
				}}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/index">>) ->
				{ok, #tx {
					id = ?MANIFEST_INDEX_TXID,
					format = 2,
					tags = [{<<"Content-Type">>, <<"text/html">>}]
				}}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_data,
		fun
			(#tx{ id = ?MOCK_TXID }) ->
				{ok, index_manifest_fixture()};
			(#tx{ id = ?MANIFEST_INDEX_TXID }) ->
				{ok, <<"<html><body>Some HTML</body></html>">>}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, Body} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/index.html">>
		),
	?assertEqual(200, Status),
	?assertMatch(#{ <<"content-type">> := <<"text/html">> }, Headers),
	?assertEqual(<<"<html><body>Some HTML</body></html>">>, Body),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

handle_multi_segment_subpaths() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {ok, <<"path/to/manifest">>};
			(?MANIFEST_DOG_TXID) -> {ok, <<"path/to/dog">>}
		end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = index_manifest_fixture()
				}};
			(<<"path/to/dog">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"image/jpeg">>}],
					data = <<"some jpeg">>
				}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, Body} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/img/dog.jpg">>
		),
	?assertEqual(200, Status),
	?assertMatch(#{ <<"content-type">> := <<"image/jpeg">> }, Headers),
	?assertEqual(<<"some jpeg">>, Body),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

redirect_badly_labeled_manifest_subpaths() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {ok, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, Headers, _} =
		req(
			<<"https">>,
			<<"badlabel.", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/index.html">>
		),
	#{ <<"location">> := Location } = Headers,
	?assertEqual(<<
		"https://",
		?MOCK_TXLABEL/binary, ".",
		?MOCK_DOMAIN/binary, "/",
		Hash/binary, "/index.html"
	>>, Location),
	?assertEqual(301, Status),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

return_421_on_bad_manifest() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {migrated_v1, <<"path/to/manifest">>}
		end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = <<"inproper json">>
				}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, _, _} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/index.html">>
		),
	?assertEqual(421, Status),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

return_404_on_non_existent_subpath() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {migrated_v1, <<"path/to/manifest">>}
		end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = index_manifest_fixture()
				}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	{ok, Status, _, _} =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/bad_path">>
		),
	?assertEqual(404, Status),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

forward_on_subpath_of_non_manifest() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun(?MOCK_TXID) -> {ok, <<"some/mock/path">>} end
	),
	meck:expect(
		ar_arql_db,
		select_tx_by_id,
		fun(?MOCK_TXHASH) ->
			{ok, #{ block_indep_hash => ?MOCK_BLOCKHASH }}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun(<<"some/mock/path">>) ->
			{ok, #tx {
				tags = [{<<"Content-Type">>, <<"text/plain">>}],
				data = <<"Some mock data">>
			}}
		end
	),
	Hash = ar_util:encode(?MOCK_TXID),
	forward =
		req(
			<<"https">>,
			<<?MOCK_TXLABEL/binary, ".", ?MOCK_DOMAIN/binary>>,
			<<"/", Hash/binary, "/some/subpath">>
		),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

serve_manifest_subpath_on_custom_domain() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {migrated_v1, <<"path/to/manifest">>};
			(?MANIFEST_INDEX_TXID) -> {ok, <<"path/to/index">>}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = index_manifest_fixture()
				}}
		end
	),
	meck:expect(
		ar_storage,
		read_tx_file,
		fun
			(<<"path/to/index">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"text/html">>}],
					data = <<"<html><body>Some HTML</body></html>">>
				}}
		end
	),
	meck:expect(
		ar_domain,
		lookup_arweave_txt_record,
		fun(?MOCK_CUSTOM_DOMAIN) ->
			ar_util:encode(?MOCK_TXID)
		end
	),
	{ok, Status, Headers, Body} =
		req(
			<<"https">>,
			?MOCK_CUSTOM_DOMAIN,
			<<"/index.html">>
		),
	?assertEqual(200, Status),
	?assertMatch(#{ <<"content-type">> := <<"text/html">> }, Headers),
	?assertEqual(<<"<html><body>Some HTML</body></html>">>, Body),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

forward_non_existent_subpaths_to_node_api() ->
	meck:expect(
		ar_storage,
		lookup_tx_filename,
		fun
			(?MOCK_TXID) -> {migrated_v1, <<"path/to/manifest">>};
			(?MANIFEST_INDEX_TXID) -> {migrated_v1, <<"path/to/index">>}
		end
	),
	meck:expect(
		ar_storage,
		read_migrated_v1_tx_file,
		fun
			(<<"path/to/manifest">>) ->
				{ok, #tx {
					tags = [{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}],
					data = index_manifest_fixture()
				}}
		end
	),
	meck:expect(
		ar_domain,
		lookup_arweave_txt_record,
		fun(?MOCK_CUSTOM_DOMAIN) ->
			ar_util:encode(?MOCK_TXID)
		end
	),
	?assertEqual(forward,
	req(
		<<"https">>,
		?MOCK_CUSTOM_DOMAIN,
		<<"/other/path">>
	)),
	?assert(meck:validate(ar_storage)),
	?assert(meck:validate(ar_arql_db)),
	?assert(meck:validate(ar_domain)).

req(Scheme, Host, Path) ->
	Pid = self(),
	StreamId = make_ref(),
	Req = #{
		method => <<"GET">>,
		scheme => Scheme,
		host => Host,
		path => Path,
		pid => self(),
		streamid => StreamId
	},
	case ar_gateway_middleware:execute(Req, ?MOCK_ENV) of
		{stop, _} ->
			receive
				{{Pid, StreamId}, {response, Status, Headers, Body}} ->
					{ok, Status, Headers, iolist_to_binary(Body)}
			end;
		{ok, _, _} ->
			forward
	end.

index_manifest_fixture() ->
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_gateway_middleware_tests_index_manifest_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.

listing_manifest_fixture() ->
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_gateway_middleware_tests_listing_manifest_fixture.json"),
	{ok, FileData} = file:read_file(Path),
	FileData.
