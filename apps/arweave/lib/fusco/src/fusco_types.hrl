%%% ----------------------------------------------------------------------------
%%% Copyright (c) 2009, Erlang Training and Consulting Ltd.
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%    * Redistributions of source code must retain the above copyright
%%%      notice, this list of conditions and the following disclaimer.
%%%    * Redistributions in binary form must reproduce the above copyright
%%%      notice, this list of conditions and the following disclaimer in the
%%%      documentation and/or other materials provided with the distribution.
%%%    * Neither the name of Erlang Training and Consulting Ltd. nor the
%%%      names of its contributors may be used to endorse or promote products
%%%      derived from this software without specific prior written permission.
%%%
%%% THIS SOFTWARE IS PROVIDED BY Erlang Training and Consulting Ltd. ''AS IS''
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED. IN NO EVENT SHALL Erlang Training and Consulting Ltd. BE
%%% LIABLE SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
%%% BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
%%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%% ----------------------------------------------------------------------------

-type path() :: binary().

-type header() :: binary().

-type headers() :: [{header(), iodata()}].

-type method() :: iodata().

-type pos_timeout() ::  pos_integer() | 'infinity'.

-type socket() :: _.

-type port_num() :: 1..65535.

-type invalid_option() :: any().

-type destination() :: {string(), pos_integer(), boolean()}.

-type option() ::
        {'connect_timeout', timeout()} |
        {'send_retry', non_neg_integer()} |
        {'connect_options', socket_options()} |
        {'use_cookies', boolean()} |
        {'proxy', string()} |
        {'proxy_ssl_options', socket_options()} |
        invalid_option().

-type options() :: [option()].

-type host() :: string() | {integer(), integer(), integer(), integer()}.

-type socket_options() :: [{atom(), term()} | atom()].

-type body()         :: binary()    |
                        'undefined' | % HEAD request.
                        pid().        % When partial_download option is used.

-type result() ::
        {ok, {{binary(), binary()}, headers(), body(),
              non_neg_integer(), pos_timeout()}} |
        {error, atom()}.
