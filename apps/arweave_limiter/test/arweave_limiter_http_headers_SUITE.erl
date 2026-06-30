-module(arweave_limiter_http_headers_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_http_headers).
-define(TEST_LIMITER, "test_limiter").
-define(POLICIES, #{id => ?TEST_LIMITER,
                    concurrency => #{limit => 500},
                    sliding_window => #{limit => 10,
                                        window_seconds => 1},
                    leaky_bucket   => #{burst => 450,
                                        tick_ms => 30000,
                                        tick_reduction => 450}}).

suite() -> [{userdata, [description()]}].

description() -> {description, "arweave_limiter_http_headers test interface"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(_TestCase, _Config) -> ok.

all() ->
	[
		disabled,
		register,
		reject
	].

disabled(_Config) ->
	?assertEqual(#{}, ?M:to_http_headers({register, no_limiting_applied, #{policies => ?POLICIES}})),
	ok.

register(_Config) ->
	?assertEqual(
	   #{<<"RateLimit-Limit">> =>
	         <<"10, 10;w=1;policy=\"test_limiter sliding window\", 450;w=1;burst=450;policy=\"test_limiter leaky bucket\" 500;w=1;policy=\"test_limiter concurrency\" ">>,
	     <<"RateLimit-Remaining">> => <<"9">>,
	     <<"RateLimit-Reset">> => <<"1">>},
	   ?M:to_http_headers({register, sliding,
	                       #{expiring_limit => 10,
	                         remaining      => 9,
	                         reset_seconds  => 1,
	                         policies => ?POLICIES}
	                      })),
	?assertEqual(
	   #{<<"RateLimit-Limit">> =>
	         <<"450, 10;w=1;policy=\"test_limiter sliding window\", 450;w=1;burst=450;policy=\"test_limiter leaky bucket\" 500;w=1;policy=\"test_limiter concurrency\" ">>,
	     <<"RateLimit-Remaining">> => <<"449">>,
	     <<"RateLimit-Reset">> => <<"29">>},
	   ?M:to_http_headers({register, leaky,
	                       #{expiring_limit => 450,
	                         remaining      => 449,
	                         reset_seconds  => 29,
	                         policies => ?POLICIES}
	                      })),
	ok.

reject(_Config) ->
	?assertEqual(
	   #{<<"RateLimit-Limit">> =>
	         <<"500, 10;w=1;policy=\"test_limiter sliding window\", 450;w=1;burst=450;policy=\"test_limiter leaky bucket\" 500;w=1;policy=\"test_limiter concurrency\" ">>,
	     <<"RateLimit-Remaining">> => <<"0">>,
	     <<"RateLimit-Reset">> => <<"1">>},
	   ?M:to_http_headers({register, concurrency,
	                       #{expiring_limit => 500,
	                         remaining      => 0,
	                         reset_seconds  => 1,
	                         policies => ?POLICIES}
	                      })),
	?assertEqual(
	   #{<<"RateLimit-Limit">> =>
	         <<"450, 10;w=1;policy=\"test_limiter sliding window\", 450;w=1;burst=450;policy=\"test_limiter leaky bucket\" 500;w=1;policy=\"test_limiter concurrency\" ">>,
	     <<"RateLimit-Remaining">> => <<"0">>,
	     <<"RateLimit-Reset">> => <<"15">>},
	   ?M:to_http_headers({register, rate_limit,
	                       #{expiring_limit => 450,
	                         remaining      => 0,
	                         reset_seconds  => 15,
	                         policies => ?POLICIES}
	                      })),
	ok.
