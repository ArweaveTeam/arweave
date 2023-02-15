-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-include_lib("eunit/include/eunit.hrl").

no_services_validate_test() ->
	ServicesConfig = [],
	?assertEqual(
		{ok, ServicesConfig},
		ar_p3:validate_config(config_fixture(ServicesConfig))).

basic_validate_test() ->
	ServicesConfig = [
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
	?assertEqual(
		{ok, ServicesConfig},
		ar_p3:validate_config(config_fixture(ServicesConfig))).

no_endpoint_validate_test() ->
	ServicesConfig = [
		#p3_service{
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
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_endpoint_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"https://mydomain.com/info">>,
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
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_mod_seq_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_mod_seq_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = "1",
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_rates_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_rates_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = 1
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_rate_type_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_rate_type_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = "invalid",
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_arweave_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_arweave_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = 1
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).


no_ar_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_ar_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #{}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_ar_price_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_ar_price_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"abc">>,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

string_ar_price_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = "1000",
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

integer_ar_price_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = 1000,
						address = <<"abc">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

no_ar_address_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).

bad_ar_address_validate_test() ->
	ServicesConfig = [
		#p3_service{
			endpoint = <<"/info">>,
			mod_seq = 1,
			rates = #p3_rates{
				rate_type = <<"request">>,
				arweave = #p3_arweave{
					ar = #p3_ar{
						price = <<"1000">>,
						address = <<"no good">>
					}
				}
			}
		}
	],
	?assertMatch(
		{stop, _},
	 	ar_p3:validate_config(config_fixture(ServicesConfig))).


config_fixture(Services) ->
	#config{ services = Services }.