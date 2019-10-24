// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.

#include "jiffy.h"

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    jiffy_st* st = enif_alloc(sizeof(jiffy_st));
    if(st == NULL) {
        return 1;
    }

    st->atom_ok = make_atom(env, "ok");
    st->atom_error = make_atom(env, "error");
    st->atom_null = make_atom(env, "null");
    st->atom_true = make_atom(env, "true");
    st->atom_false = make_atom(env, "false");
    st->atom_bignum = make_atom(env, "bignum");
    st->atom_bignum_e = make_atom(env, "bignum_e");
    st->atom_bigdbl = make_atom(env, "bigdbl");
    st->atom_partial = make_atom(env, "partial");
    st->atom_uescape = make_atom(env, "uescape");
    st->atom_pretty = make_atom(env, "pretty");
    st->atom_force_utf8 = make_atom(env, "force_utf8");
    st->atom_iter = make_atom(env, "iter");
    st->atom_bytes_per_iter = make_atom(env, "bytes_per_iter");
    st->atom_return_maps = make_atom(env, "return_maps");
    st->atom_return_trailer = make_atom(env, "return_trailer");
    st->atom_has_trailer = make_atom(env, "has_trailer");
    st->atom_nil = make_atom(env, "nil");
    st->atom_use_nil = make_atom(env, "use_nil");
    st->atom_null_term = make_atom(env, "null_term");
    st->atom_escape_forward_slashes = make_atom(env, "escape_forward_slashes");
    st->atom_dedupe_keys = make_atom(env, "dedupe_keys");
    st->atom_copy_strings = make_atom(env, "copy_strings");

    // Markers used in encoding
    st->ref_object = make_atom(env, "$object_ref$");
    st->ref_array = make_atom(env, "$array_ref$");

    st->res_dec = enif_open_resource_type(
            env,
            NULL,
            "decoder",
            dec_destroy,
            ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
            NULL
        );

    st->res_enc = enif_open_resource_type(
            env,
            NULL,
            "encoder",
            enc_destroy,
            ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
            NULL
        );

    *priv = (void*) st;

    return 0;
}

static int
reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    return load(env, priv, info);
}

static void
unload(ErlNifEnv* env, void* priv)
{
    enif_free(priv);
    return;
}

static ErlNifFunc funcs[] =
{
    {"nif_decode_init", 2, decode_init},
    {"nif_decode_iter", 5, decode_iter},
    {"nif_encode_init", 2, encode_init},
    {"nif_encode_iter", 3, encode_iter}
};

ERL_NIF_INIT(jiffy, funcs, &load, &reload, &upgrade, &unload);
