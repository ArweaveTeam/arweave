Base64Url
==============

[![Hex.pm](https://img.shields.io/hexpm/v/base64url.svg)](https://hex.pm/packages/base64url)

Standalone [URL safe](http://tools.ietf.org/html/rfc4648) base64-compatible codec.

Usage
--------------

URL-Safe base64 encoding:
```erlang
base64url:encode(<<255,127,254,252>>).
<<"_3_-_A">>
base64url:decode(<<"_3_-_A">>).
<<255,127,254,252>>
```

Vanilla base64 encoding:
```erlang
base64:encode(<<255,127,254,252>>).
<<"/3/+/A==">>
```

Some systems in the wild use base64 URL encoding, but keep the padding for MIME compatibility (base64 Content-Transfer-Encoding). To interact with such systems, use:
```erlang
base64url:encode_mime(<<255,127,254,252>>).
<<"_3_-_A==">>
base64url:decode(<<"_3_-_A==">>).
<<255,127,254,252>>
```

Thanks
--------------

To authors of [this](https://github.com/basho/riak_control/blob/master/src/base64url.erl) and [this](https://github.com/mochi/mochiweb/blob/master/src/mochiweb_base64url.erl).

[License](base64url/blob/master/LICENSE.txt)
-------

Copyright (c) 2013 Vladimir Dronnikov <dronnikov@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
