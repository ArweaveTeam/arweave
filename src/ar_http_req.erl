-module(ar_http_req).
-include("ar_http_req.hrl").
-export([body/1]).

body(#{ ?AR_HTTP_REQ_BODY := Body }) -> Body.
