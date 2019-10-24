FUSCO
=====

Fast and Ultra Slim Connection Oriented HTTP Client

Fusco is an Erlang HTTP client for high performance applications.
For all those who need a generic HTTP client, check [lhttpc](https://github.com/esl/lhttpc) in which development is based Fusco. Not all the functionalities of lhttpc are present here.

Fusco is still in a very early stage of development, be aware of that!

Example
-----

Assume that you have a web server at `www.example.com`, and you want to perform `GET /example`. For that you need to:
1. Spawn client (client is just a pid)
```erlang
1> {ok, Client} = fusco:start("http://www.example.com", []).
{ok,<0.43.0>}
```
2. Use `request` function - we perform `GET /example` so:
```erlang
2> {ok, Result} = fusco:request(Client, <<"/example">>, <<"GET">>, [], [], 5000).
```
3. After all request to server - close the connection:
```erlang
3> fusco:disconnect(Client).
ok
```

Usage
----
To perform request you need only to know 3 functions from `fusco` module:
* `start/2` - spawns a new `Client` (a `gen_server`) which is associated to server on `Host`
```erlang
start(Host :: string(), Options :: []) -> {ok, Client :: pid()}
``` 
* `request/6` - makes a request on a `Path` of `Host` of web server which is `Client` associated for. Every `Path` must be preceeded by `/`. Methods must be written in uppercase e.g. `<<"GET">>`
```erlang
request(Client, Path, Method, Headers, Body, Timeout) -> Result

Client = pid()
Host = binary()
Method = binary()
Hdrs = [{Header, Value}]
Header = string() | binary() | atom()
Value = string() | binary()
RequestBody = iodata()
RetryCount = integer()
Timeout = integer() | infinity
Result = {ok, {{StatusCode, ReasonPhrase}, Hdrs, ResponseBody}}
         | {error, Reason}
StatusCode = integer()
ReasonPhrase = string()
ResponseBody = binary() | pid() | undefined
Reason = connection_closed | connect_timeout | timeout
``` 

Possible `Options` for `start` function:

`{connect_timeout, Milliseconds}` specifies how many milliseconds the
client can spend trying to establish a connection to the server. This
doesn't affect the overall request timeout. However, if it's longer than
the overall timeout it will be ignored. Also note that the TCP layer may
choose to give up earlier than the connect timeout, in which case the
client will also give up. The default value is infinity, which means that
it will either give up when the TCP stack gives up, or when the overall
request timeout is reached.

`{connect_options, Options}` specifies options to pass to the socket at
connect time. This makes it possible to specify both SSL options and
regular socket options, such as which IP/Port to connect from etc.
Some options must not be included here, namely the mode, `binary`
or `list`, `{active, boolean()}`, `{active, once}` or `{packet, Packet}`.
These options would confuse the client if they are included.
Please note that these options will only have an effect on *new*
connections, and it isn't possible for different requests
to the same host uses different options unless the connection is closed
between the requests. Using HTTP/1.0 or including the "Connection: close"
header would make the client close the connection after the first
response is received.

`{send_retry, N}` specifies how many times the client should retry
sending a request if the connection is closed after the data has been
sent. The default value is 1.
 
`{proxy, ProxyUrl}` if this option is specified, a proxy server is used as
an intermediary for all communication with the destination server. The link
to the proxy server is established with the HTTP CONNECT method (RFC2817).
Example value: `{proxy, "http://john:doe@myproxy.com:3128"}`
 
`{proxy_ssl_options, SslOptions}` this is a list of SSL options to use for
the SSL session created after the proxy connection is established. For a
list of all available options, please check OTP's ssl module manpage.
* `disconnect/1` - disconnects the `Client` from server. After that it kills the `Client` process
```erlang
disconnect(Client :: pid())
``` 

Running Tests
----
You can execute all test with
```bash
> make test
```
And you can check [this repo](https://github.com/esl/fusco_eqc) and run the Erlang QuickCheck tests