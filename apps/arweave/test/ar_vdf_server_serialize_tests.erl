-module(ar_vdf_server_serialize_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

serialize_test_() ->
	[
		{timeout, 120, fun ar_vdf_server_tests:test_serialize_update_format_2/0},
		{timeout, 120, fun ar_vdf_server_tests:test_serialize_update_format_3/0},
		{timeout, 120, fun ar_vdf_server_tests:test_serialize_update_format_4/0},
		{timeout, 120, fun ar_vdf_server_tests:test_serialize_response/0},
		{timeout, 120, fun ar_vdf_server_tests:test_serialize_response_compatibility/0}
	].
