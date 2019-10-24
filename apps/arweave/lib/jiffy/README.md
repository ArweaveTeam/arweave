Jiffy - JSON NIFs for Erlang
============================

A JSON parser as a NIF. This is a complete rewrite of the work I did
in EEP0018 that was based on Yajl. This new version is a hand crafted
state machine that does its best to be as quick and efficient as
possible while not placing any constraints on the parsed JSON.

[![Build Status](https://travis-ci.org/davisp/jiffy.svg?branch=master)](https://travis-ci.org/davisp/jiffy)

Usage
-----

Jiffy is a simple API. The only thing that might catch you off guard
is that the return type of `jiffy:encode/1` is an iolist even though
it returns a binary most of the time.

A quick note on unicode. Jiffy only understands UTF-8 in binaries. End
of story.

Errors are raised as exceptions.

    Eshell V5.8.2  (abort with ^G)
    1> jiffy:decode(<<"{\"foo\": \"bar\"}">>).
    {[{<<"foo">>,<<"bar">>}]}
    2> Doc = {[{foo, [<<"bing">>, 2.3, true]}]}.
    {[{foo,[<<"bing">>,2.3,true]}]}
    3> jiffy:encode(Doc).
    <<"{\"foo\":[\"bing\",2.3,true]}">>

`jiffy:decode/1,2`
------------------

* `jiffy:decode(IoData)`
* `jiffy:decode(IoData, Options)`

The options for decode are:

* `return_maps` - Tell Jiffy to return objects using the maps data type
  on VMs that support it. This raises an error on VMs that don't support
  maps.
* `{null_term, Term}` - Returns the specified `Term` instead of `null`
  when decoding JSON. This is for people that wish to use `undefined`
  instead of `null`.
* `use_nil` - Returns the atom `nil` instead of `null` when decoding
  JSON. This is a short hand for `{null_term, nil}`.
* `return_trailer` - If any non-whitespace is found after the first
  JSON term is decoded the return value of decode/2 becomes
  `{has_trailer, FirstTerm, RestData::iodata()}`. This is useful to
  decode multiple terms in a single binary.
* `dedupe_keys` - If a key is repeated in a JSON object this flag
  will ensure that the parsed object only contains a single entry
  containing the last value seen. This mirrors the parsing beahvior
  of virtually every other JSON parser.
* `copy_strings` - Normaly when strings are decoded they are created
  as sub-binaries of the input data. With some workloads this can lead
  to an undeseriable bloating of memory when a few small strings in JSON
  keep a reference to the full JSON document alive. Setting this option
  will instead allocate new binaries for each string to avoid keeping
  the original JSON document around after garbage collection.
* `{bytes_per_red, N}` where N &gt;= 0 - This controls the number of
  bytes that Jiffy will process as an equivalent to a reduction. Each
  20 reductions we consume 1% of our allocated time slice for the current
  process. When the Erlang VM indicates we need to return from the NIF.
* `{bytes_per_iter, N}` where N &gt;= 0 - Backwards compatible option
  that is converted into the `bytes_per_red` value.

`jiffy:encode/1,2`
------------------

* `jiffy:encode(EJSON)`
* `jiffy:encode(EJSON, Options)`

where EJSON is a valid representation of JSON in Erlang according to
the table below.

The options for encode are:

* `uescape` - Escapes UTF-8 sequences to produce a 7-bit clean output
* `pretty` - Produce JSON using two-space indentation
* `force_utf8` - Force strings to encode as UTF-8 by fixing broken
  surrogate pairs and/or using the replacement character to remove
  broken UTF-8 sequences in data.
* `use_nil` - Encode's the atom `nil` as `null`.
* `escape_forward_slashes` - Escapes the `/` character which can be
  useful when encoding URLs in some cases.
* `{bytes_per_red, N}` - Refer to the decode options
* `{bytes_per_iter, N}` - Refer to the decode options

Data Format
-----------

    Erlang                          JSON            Erlang
    ==========================================================================

    null                       -> null           -> null
    true                       -> true           -> true
    false                      -> false          -> false
    "hi"                       -> [104, 105]     -> [104, 105]
    <<"hi">>                   -> "hi"           -> <<"hi">>
    hi                         -> "hi"           -> <<"hi">>
    1                          -> 1              -> 1
    1.25                       -> 1.25           -> 1.25
    []                         -> []             -> []
    [true, 1.0]                -> [true, 1.0]    -> [true, 1.0]
    {[]}                       -> {}             -> {[]}
    {[{foo, bar}]}             -> {"foo": "bar"} -> {[{<<"foo">>, <<"bar">>}]}
    {[{<<"foo">>, <<"bar">>}]} -> {"foo": "bar"} -> {[{<<"foo">>, <<"bar">>}]}
    #{<<"foo">> => <<"bar">>}  -> {"foo": "bar"} -> #{<<"foo">> => <<"bar">>}

N.B. The last entry in this table is only valid for VM's that support
the `maps` data type (i.e., 17.0 and newer) and client code must pass
the `return_maps` option to `jiffy:decode/2`.

Improvements over EEP0018
-------------------------

Jiffy should be in all ways an improvement over EEP0018. It no longer
imposes limits on the nesting depth. It is capable of encoding and
decoding large numbers and it does quite a bit more validation of UTF-8 in strings.

