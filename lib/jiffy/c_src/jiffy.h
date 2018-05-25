// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.

#ifndef JIFFY_H
#define JIFFY_H

#include "erl_nif.h"

#define DEFAULT_BYTES_PER_REDUCTION 20
#define DEFAULT_ERLANG_REDUCTION_COUNT 2000

#define MAP_TYPE_PRESENT \
    ((ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION >= 6) \
    || (ERL_NIF_MAJOR_VERSION > 2))

typedef struct {
    ERL_NIF_TERM    atom_ok;
    ERL_NIF_TERM    atom_error;
    ERL_NIF_TERM    atom_null;
    ERL_NIF_TERM    atom_true;
    ERL_NIF_TERM    atom_false;
    ERL_NIF_TERM    atom_bignum;
    ERL_NIF_TERM    atom_bignum_e;
    ERL_NIF_TERM    atom_bigdbl;
    ERL_NIF_TERM    atom_partial;
    ERL_NIF_TERM    atom_uescape;
    ERL_NIF_TERM    atom_pretty;
    ERL_NIF_TERM    atom_force_utf8;
    ERL_NIF_TERM    atom_iter;
    ERL_NIF_TERM    atom_bytes_per_iter;
    ERL_NIF_TERM    atom_return_maps;
    ERL_NIF_TERM    atom_return_trailer;
    ERL_NIF_TERM    atom_has_trailer;
    ERL_NIF_TERM    atom_nil;
    ERL_NIF_TERM    atom_use_nil;
    ERL_NIF_TERM    atom_null_term;
    ERL_NIF_TERM    atom_escape_forward_slashes;
    ERL_NIF_TERM    atom_dedupe_keys;
    ERL_NIF_TERM    atom_copy_strings;

    ERL_NIF_TERM    ref_object;
    ERL_NIF_TERM    ref_array;

    ErlNifResourceType* res_dec;
    ErlNifResourceType* res_enc;
} jiffy_st;

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name);
ERL_NIF_TERM make_ok(jiffy_st* st, ErlNifEnv* env, ERL_NIF_TERM data);
ERL_NIF_TERM make_error(jiffy_st* st, ErlNifEnv* env, const char* error);
ERL_NIF_TERM make_obj_error(jiffy_st* st, ErlNifEnv* env, const char* error,
        ERL_NIF_TERM obj);
int get_bytes_per_iter(ErlNifEnv* env, ERL_NIF_TERM val, size_t* bpi);
int get_bytes_per_red(ErlNifEnv* env, ERL_NIF_TERM val, size_t* bpr);
int get_null_term(ErlNifEnv* env, ERL_NIF_TERM val, ERL_NIF_TERM *null_term);
int should_yield(ErlNifEnv* env, size_t* used, size_t bytes_per_red);

ERL_NIF_TERM decode_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM decode_iter(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM encode_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM encode_iter(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

void dec_destroy(ErlNifEnv* env, void* obj);
void enc_destroy(ErlNifEnv* env, void* obj);

int make_object(ErlNifEnv* env, ERL_NIF_TERM pairs, ERL_NIF_TERM* out,
        int ret_map, int dedupe_keys);

int int_from_hex(const unsigned char* p);
int int_to_hex(int val, char* p);
int utf8_len(int c);
int utf8_esc_len(int c);
int utf8_validate(unsigned char* data, size_t size);
int utf8_to_unicode(unsigned char* buf, size_t size);
int unicode_to_utf8(int c, unsigned char* buf);
int unicode_from_pair(int hi, int lo);
int unicode_uescape(int c, char* buf);
int double_to_shortest(char *buf, size_t size, size_t* len, double val);

#endif // Included JIFFY_H
