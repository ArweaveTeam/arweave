%%%===================================================================
%%% @doc This is a draft. The idea is to create an interface to
%%% configure dynamic keys (including variable). For example:
%%%
%%% ```
%%% set("peers.[127.0.0.1:1234].enabled", true).
%%% set("peers.[127.0.0.2:1234].enabled", true).
%%% set("peers.[127.0.0.3:1234].enabled", true).
%%% '''
%%%
%%% The second element of the key is dynamic and can be set to
%%% different kind of value. Then, the `configuration_key' should
%%% reflect this:
%%%
%%% '''
%%% configuration_key() -> [peers,{},enabled].
%%% % or
%%% configuration_key() -> [peers,{peer},enabled].
%%% ```
%%%
%%% another method is to defined an atom starting with a special
%%% letter like:
%%%
%%% '''
%%% configuration_key() -> [peers, '@peer', enabled].
%%% ```
%%%
%%% it will also need to sanitize the configuration key. In the case
%%% of peer, we should define it clearly:
%%%
%%%  - a peer can be an IPv4 address only
%%%
%%%    1.2.3.4
%%%  
%%%  - a peer can be an Ipv4 address with a port
%%%
%%%    1.2.3.4:1234
%%%  
%%%  - a peer can be a domain name only. the domain name will be then
%%%    converted as an IPv4 with port
%%%
%%%    mypeer.com
%%%
%%%  - a peer can be a domain name only with a port. the domain name
%%%    will be then converted as an Ipv4 with a port
%%%
%%%    mypeer.com:1234
%%%
%%%  - a peer can be a domain name regrouping multiple ipv4. In this
%%%    case the parameters will apply on all IPv4/port addresses.
%%%
%%%
%%% @end
%%%===================================================================
-module(arweave_config_peers_enabled).
-behavior(arweave_config_spec).
-export([
	deprecated/0,
	type/0,
	default/0,
	required/0,
	legacy/0,
	runtime/0,
	short_description/0,
	long_description/0,
	configuration_key/0,
	check/2,
	handle_get/1,
	handle_set/2
]).

configuration_key() -> {ok, [peers,{peer},enabled]}.

runtime() -> true.

deprecated() -> false.

type() -> {ok, boolean}.

default() -> {ok, true}.

required() -> {ok, false}.

legacy() -> {ok, []}.

short_description() -> {ok, []}.

long_description() -> {ok, []}.

check(_Key, Value) when is_boolean(Value) -> ok;
check(_, _) -> {error, bad_value}.

handle_get(_Key) ->
	{ok, value}.

handle_set(_Key, Value) ->
	{ok, Value}.
