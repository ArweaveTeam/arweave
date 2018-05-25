// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.

#include <set>
#include <string>

#include <assert.h>

#include "erl_nif.h"

#define MAP_TYPE_PRESENT \
    ((ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION >= 6) \
    || (ERL_NIF_MAJOR_VERSION > 2))

#define BEGIN_C extern "C" {
#define END_C }

BEGIN_C

int
make_object(ErlNifEnv* env, ERL_NIF_TERM pairs, ERL_NIF_TERM* out,
        int ret_map, int dedupe_keys)
{
    ERL_NIF_TERM ret;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;

    std::set<std::string> seen;

#if MAP_TYPE_PRESENT
    if(ret_map) {
        ret = enif_make_new_map(env);
        while(enif_get_list_cell(env, pairs, &val, &pairs)) {
            if(!enif_get_list_cell(env, pairs, &key, &pairs)) {
                assert(0 == 1 && "Unbalanced object pairs.");
            }
            ErlNifBinary bin;
            if(!enif_inspect_binary(env, key, &bin)) {
                return 0;
            }
            std::string skey((char*) bin.data, bin.size);
            if(seen.count(skey) == 0) {
                seen.insert(skey);
                if(!enif_make_map_put(env, ret, key, val, &ret)) {
                    return 0;
                }
            }
        }
        *out = ret;
        return 1;
    }
#endif

    ret = enif_make_list(env, 0);
    while(enif_get_list_cell(env, pairs, &val, &pairs)) {
        if(!enif_get_list_cell(env, pairs, &key, &pairs)) {
            assert(0 == 1 && "Unbalanced object pairs.");
        }
        if(dedupe_keys) {
            ErlNifBinary bin;
            if(!enif_inspect_binary(env, key, &bin)) {
                return 0;
            }
            std::string skey((char*) bin.data, bin.size);
            if(seen.count(skey) == 0) {
                seen.insert(skey);
                val = enif_make_tuple2(env, key, val);
                ret = enif_make_list_cell(env, val, ret);
            }
        } else {
            val = enif_make_tuple2(env, key, val);
            ret = enif_make_list_cell(env, val, ret);
        }
    }
    *out = enif_make_tuple1(env, ret);

    return 1;
}

END_C
