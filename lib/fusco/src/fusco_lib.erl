%%%-----------------------------------------------------------------------------
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @author Ramon Lastres Guerrero <ramon.lastres@erlang-solutions.com>
%%% @doc This module implements various library functions used in fusco
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco_lib).
-copyright("2013, Erlang Solutions Ltd.").

-export([parse_url/1,
         format_request/6,
         header_value/2,
         update_cookies/2,
         delete_expired_cookies/2,
         to_lower/1,
         get_value/2,
         get_value/3,
         host_header/2,
         is_close/1,
         maybe_ipv6_enclose/1]).

-include("fusco_types.hrl").
-include("fusco.hrl").

-define(HTTP_LINE_END, <<"\r\n">>).

-type fusco_url() :: #fusco_url{}.
-type fusco_cookie() :: #fusco_cookie{}.

%%==============================================================================
%% Exported functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @spec header_value(Header, Headers) -> undefined | term()
%% Header = string()
%% Headers = [{header(), term()}]
%% Value = term()
%% @doc
%% Returns the value associated with the `Header' in `Headers'.
%% `Header' must be a lowercase string, since every header is mangled to
%% check the match.
%% @end
%%------------------------------------------------------------------------------
-spec header_value(string() | binary(), headers()) -> undefined | term().
header_value(Hdr, Hdrs) ->
    %% TODO ensure headers and values are stripped
    case lists:keyfind(Hdr, 1, Hdrs) of
        false ->
            undefined;
        {Hdr, Value} ->
            Value
    end.

%%------------------------------------------------------------------------------
%% @spec (URL) -> fusco_url()
%%   URL = string()
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec parse_url(string()) -> fusco_url().
parse_url(URL) ->
    % XXX This should be possible to do with the re module?
    {Scheme, CredsHostPortPath} = split_scheme(URL),
    {User, Passwd, HostPortPath} = split_credentials(CredsHostPortPath),
    {Host, PortPath} = split_host(HostPortPath, []),
    {Port, Path} = split_port(Scheme, PortPath, []),
    #fusco_url{host = fusco_lib:to_lower(Host), port = Port, path = Path,
                user = User, password = Passwd, is_ssl = (Scheme =:= https)}.

%%------------------------------------------------------------------------------
%% @spec (Path, Method, Headers, Host, Body, Cookies) ->
%%    Request
%% Path = path()
%% Method = atom() | string()
%% Headers = [{atom() | string(), string()}]
%% Host = string()
%% Body = iolist()
%% Cookies = [fusco_cookie()]
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec format_request(path(), method(), headers(), string(),  iolist(),
                     {boolean(), [fusco_cookie()]}) -> {iodata(), iodata()}.
format_request(Path, Method, Hdrs, Host, Body, Cookies) ->
    {AllHdrs, ConHdr} =
        add_mandatory_hdrs(Path, Hdrs, Host, Body, Cookies),
    {[Method, <<" ">>, Path, <<" HTTP/1.1">>, ?HTTP_LINE_END, AllHdrs,
     ?HTTP_LINE_END, Body], ConHdr}.

%%------------------------------------------------------------------------------
%% @private
%% @doc Updated the state of the cookies. after we receive a response.
%% @end
%%------------------------------------------------------------------------------
-spec update_cookies(headers(), [fusco_cookie()]) -> [fusco_cookie()].
update_cookies([], []) ->
    [];
update_cookies([], StateCookies) ->
    StateCookies;
update_cookies(ReceivedCookies, []) ->
    ReceivedCookies;
update_cookies(ReceivedCookies, StateCookies) ->
    %% substitute the cookies with the same name, add the others, delete.

    %% http://tools.ietf.org/search/rfc6265#section-4.1.2
    %% If a user agent receives a Set-Cookie response header whose NAME is
    %% the same as a pre-existing cookie, and whose Domain and Path
    %% attribute values exactly (string) match those of a pre-existing
    %% cookie, the new cookie supersedes the old.
    lists:foldl(fun(NewCookie, Acc) ->
                        OldCookie =
                            lists:keyfind(NewCookie#fusco_cookie.name,
                                          #fusco_cookie.name, Acc),
                        replace_or_add_cookie(OldCookie, NewCookie, Acc)
                end, StateCookies, ReceivedCookies).

%% http://tools.ietf.org/search/rfc6265#section-4.1.2
replace_or_add_cookie(false, NewCookie, List) ->
    %% Add new cookie
    [NewCookie | List];
replace_or_add_cookie(#fusco_cookie{domain = Domain, path = Path},
                      #fusco_cookie{domain = Domain,
                                    path = Path} = NewCookie, List) ->
    %% Replace previous cookie
    lists:keystore(NewCookie#fusco_cookie.name, #fusco_cookie.name, List,
                   NewCookie);
replace_or_add_cookie(_, NewCookie, List) ->
    %% Add new cookie, path and/or domain are different
    [NewCookie | List].

%%------------------------------------------------------------------------------
%% @doc Converts characters in a string ro lower case.
%% @end
%%------------------------------------------------------------------------------
-spec to_lower(string()) -> string().
to_lower(String) when is_list(String) ->
    [char_to_lower(X) || X <- String].

bin_to_lower(Bin) ->
    << <<(char_to_lower(B))>> || <<B>> <= Bin >>.

%%------------------------------------------------------------------------------
%% @doc Compares header values to pre-defined values
%% Faster than string:to_lower and then compare
%% @end
%%------------------------------------------------------------------------------
is_close(<<"close">>) ->
    true;
is_close(<<"Close">>) ->
    true;
is_close(<<"keep-alive">>) ->
    false;
is_close(<<"Keep-Alive">>) ->
    false;
is_close(C) ->
    is_close(C, "close").

is_close(<<C, Rest1/bits>>, [C | Rest2]) ->
    is_close(Rest1, Rest2);
is_close(<<C1, Rest1/bits>>, [C2 | Rest2]) ->
    case close_to_lower(C1) == C2 of
        true ->
            is_close(Rest1, Rest2);
        false ->
            false
    end;
is_close(<<>>, _) ->
    false;
is_close(_, []) ->
    false.

close_to_lower($C) ->
    $c;
close_to_lower($L) ->
    $l;
close_to_lower($O) ->
    $o;
close_to_lower($S) ->
    $s;
close_to_lower($E) ->
    $e;
close_to_lower(C) ->
    C.

%%------------------------------------------------------------------------------
%% @doc Gets value from tuple list
%% @end
%%------------------------------------------------------------------------------
-spec get_value(Key, List) -> term() when
      Key :: term(),
      List :: [term()].
get_value(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} ->
            Value;
        false ->
            undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Gets value from tuple list. If it is not present, returns default value.
%% @end
%%------------------------------------------------------------------------------
-spec get_value(Key, List, Default) -> term() when
      Key :: term(),
      List :: [term()],
      Default :: term().
get_value(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.

%%------------------------------------------------------------------------------
%% @private
%% @doc Delete the cookies that are expired (check max-age and expire fields).
%% @end
%%------------------------------------------------------------------------------
-spec delete_expired_cookies([fusco_cookie()], erlang:timestamp()) ->
    [fusco_cookie()].
delete_expired_cookies([], _InTimestamp) ->
    [];
delete_expired_cookies(Cookies, InTimestamp) ->
    [ X || X <- Cookies, not expires(X, InTimestamp)].

%%==============================================================================
%% Internal functions
%%==============================================================================
%% http://tools.ietf.org/search/rfc6265#section-4.1.2.2
%% The Max-Age attribute indicates the maximum lifetime of the cookie,
%% represented as the number of seconds until the cookie expires.
%%
%% If a cookie has both the Max-Age and the Expires attribute,
%% the Max-Age attribute has precedence and controls the expiration date of the
%% cookie. If a cookie has neither the Max-Age nor the Expires attribute,
%% the user agent will retain the cookie until "the current session is over"
expires(#fusco_cookie{max_age = 0}, _) ->
    true;
expires(#fusco_cookie{max_age = Max}, InTimestamp) when Max =/= undefined ->
    timer:now_diff(os:timestamp(), InTimestamp) > Max;
expires(#fusco_cookie{expires = Exp}, _) when Exp =/= undefined ->
    calendar:universal_time() > Exp;
expires(_, _) ->
    false.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
split_scheme("http://" ++ HostPortPath) ->
    {http, HostPortPath};
split_scheme("https://" ++ HostPortPath) ->
    {https, HostPortPath}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
split_credentials(CredsHostPortPath) ->
    case string:tokens(CredsHostPortPath, "@") of
        [HostPortPath] ->
            {"", "", HostPortPath};
        [Creds, HostPortPath] ->
            % RFC1738 (section 3.1) says:
            % "The user name (and password), if present, are followed by a
            % commercial at-sign "@". Within the user and password field,
            % any ":", "@", or "/" must be encoded."
            % The mentioned encoding is the "percent" encoding.
            case string:tokens(Creds, ":") of
                [User] ->
                    % RFC1738 says ":password" is optional
                    {http_uri:decode(User), "", HostPortPath};
                [User, Passwd] ->
                    {http_uri:decode(User), http_uri:decode(Passwd),
                     HostPortPath}
            end
    end.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec split_host(string(), string()) -> {string(), string()}.
split_host("[" ++ Rest, []) ->
    % IPv6 address literals are enclosed by square brackets (RFC2732)
    case string:str(Rest, "]") of
        0 ->
            split_host(Rest, "[");
        N ->
            {IPv6Address, "]" ++ PortPath0} = lists:split(N - 1, Rest),
            case PortPath0 of
                ":" ++ PortPath ->
                    {IPv6Address, PortPath};
                _ ->
                    {IPv6Address, PortPath0}
            end
    end;
split_host([$: | PortPath], Host) ->
    {lists:reverse(Host), PortPath};
split_host([$/ | _] = PortPath, Host) ->
    {lists:reverse(Host), PortPath};
split_host([$? | _] = Query, Host) ->
    %% The query string follows the hostname, without a slash.  The
    %% path is empty, but for HTTP an empty path is equivalent to "/"
    %% (RFC 3986, section 6.2.3), so let's add the slash ourselves.
    {lists:reverse(Host), "/" ++ Query};
split_host([H | T], Host) ->
    split_host(T, [H | Host]);
split_host([], Host) ->
    {lists:reverse(Host), []}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%%------------------------------------------------------------------------------
split_port(http, [$/ | _] = Path, []) ->
    {80, Path};
split_port(https, [$/ | _] = Path, []) ->
    {443, Path};
split_port(http, [], []) ->
    {80, "/"};
split_port(https, [], []) ->
    {443, "/"};
split_port(_, [], Port) ->
    {list_to_integer(lists:reverse(Port)), "/"};
split_port(_,[$/ | _] = Path, Port) ->
    {list_to_integer(lists:reverse(Port)), Path};
split_port(Scheme, [P | T], Port) ->
    split_port(Scheme, T, [P | Port]).

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec add_mandatory_hdrs(path(), headers(), host(),
                         iolist(), {boolean(), [fusco_cookie()]}) ->
    {iodata(), iodata()}.
add_mandatory_hdrs(_Path, Hdrs, Host, Body, {_, []}) ->
    add_headers(Hdrs, Body, Host, undefined, []);
add_mandatory_hdrs(_Path, Hdrs, Host, Body, {false, _}) ->
    add_headers(Hdrs, Body, Host, undefined, []);
add_mandatory_hdrs(Path, Hdrs, Host, Body, {true, Cookies}) ->
    Result = {ContentHdrs, ConHdr} =
        add_headers(Hdrs, Body, Host, undefined, []),

    %% http://tools.ietf.org/search/rfc6265#section-4.1.2.4
    %% only include cookies if the cookie path is a prefix of the request path
    %% TODO optimize cookie handling
    case lists:filter(
       fun(#fusco_cookie{path_tokens = undefined}) ->
           true;
          (#fusco_cookie{path_tokens = CookiePath}) ->
               SubPath = binary:split(Path, <<"/">>, [global]),
               is_prefix(CookiePath, SubPath)
       end, Cookies)
    of
        [] ->
            Result;
        IncludeCookies ->
            {add_cookie_headers(ContentHdrs, IncludeCookies), ConHdr}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
%% http://tools.ietf.org/search/rfc6265#section-4.2.1
add_cookie_headers(Hdrs, Cookies) ->
    [[<<"Cookie: ">>, make_cookie_string(Cookies, []), ?HTTP_LINE_END]
     | Hdrs].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
make_cookie_string([], Acc) ->
    Acc;
make_cookie_string([Cookie | Rest], []) ->
    make_cookie_string(Rest, cookie_string(Cookie));
make_cookie_string([Cookie | Rest], Acc) ->
    make_cookie_string(Rest, [cookie_string(Cookie), "; " | Acc]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
cookie_string(#fusco_cookie{name = Name, value = Value}) ->
    [Name, <<"=">>, Value].

%%------------------------------------------------------------------------------
%% @private
%% Host header: http://tools.ietf.org/html/rfc2616#section-14.23
%%------------------------------------------------------------------------------
add_headers([{H, V} | T], undefined, undefined, Connection, Acc)
  when Connection =/= undefined ->
    add_headers(T, undefined, undefined, Connection,
        [[H, <<": ">>, V, ?HTTP_LINE_END] | Acc]);
add_headers([{H, V} | T], Body, Host, Connection, Acc) ->
    case bin_to_lower(H) of
        <<"connection">> ->
            add_headers(T, Body, Host, V,
                [[H, <<": ">>, V, ?HTTP_LINE_END] | Acc]);
        <<"host">> ->
            add_headers(T, Body, undefined, Connection,
                [[H, <<": ">>, V, ?HTTP_LINE_END] | Acc]);
        <<"content-length">> ->
            add_headers(T, undefined, Host, Connection,
                [[H, <<": ">>, V, ?HTTP_LINE_END] | Acc]);
        _ ->
            add_headers(T, Body, Host, Connection,
                [[H, <<": ">>, V, ?HTTP_LINE_END] | Acc])
    end;
add_headers([], undefined, Host, Connection, Headers) ->
    case Host of
        undefined ->
            {Headers, Connection};
        _ ->
            {[[<<"Host: ">>, Host, ?HTTP_LINE_END] | Headers], Connection}
    end;
add_headers([], Body, Host, Connection, Headers) ->
    ContentLength = integer_to_list(iolist_size(Body)),
    case ContentLength > 0 of
        true ->
            add_headers([], undefined, Host, Connection,
                [[<<"Content-Length: ">>, ContentLength, ?HTTP_LINE_END]
                 | Headers]);
        _ ->
            add_headers([], undefined, Host, Connection, Headers)
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec host_header(host(), port_num()) -> any().
host_header(Host, 80)   -> maybe_ipv6_enclose(Host);
% When proxying after an HTTP CONNECT session is established, squid doesn't
% like the :443 suffix in the Host header.
host_header(Host, 443)  -> maybe_ipv6_enclose(Host);
host_header(Host, Port) ->
    [maybe_ipv6_enclose(Host), $:, integer_to_list(Port)].

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec maybe_ipv6_enclose(host()) -> host().
maybe_ipv6_enclose(Host) ->
    case inet_parse:address(Host) of
        {ok, {_, _, _, _, _, _, _, _}} ->
            % IPv6 address literals are enclosed by square brackets (RFC2732)
            [$[, Host, $]];
        _ ->
            Host
    end.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%%------------------------------------------------------------------------------
char_to_lower($A) -> $a;
char_to_lower($B) -> $b;
char_to_lower($C) -> $c;
char_to_lower($D) -> $d;
char_to_lower($E) -> $e;
char_to_lower($F) -> $f;
char_to_lower($G) -> $g;
char_to_lower($H) -> $h;
char_to_lower($I) -> $i;
char_to_lower($J) -> $j;
char_to_lower($K) -> $k;
char_to_lower($L) -> $l;
char_to_lower($M) -> $m;
char_to_lower($N) -> $n;
char_to_lower($O) -> $o;
char_to_lower($P) -> $p;
char_to_lower($Q) -> $q;
char_to_lower($R) -> $r;
char_to_lower($S) -> $s;
char_to_lower($T) -> $t;
char_to_lower($U) -> $u;
char_to_lower($V) -> $v;
char_to_lower($W) -> $w;
char_to_lower($X) -> $x;
char_to_lower($Y) -> $y;
char_to_lower($Z) -> $z;
char_to_lower(Ch) -> Ch.

is_prefix([<<>>], _) ->
    true;
is_prefix([H | T1], [H | T2]) ->
    is_prefix(T1, T2);
is_prefix([], _) ->
    true;
is_prefix(_, []) ->
    false;
is_prefix(_, _) ->
    false.

