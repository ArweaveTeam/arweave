#ifndef AR_NIF_H
#define AR_NIF_H

#include <erl_nif.h>

ERL_NIF_TERM solution_tuple(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM ok_tuple(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM ok_tuple2(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
ERL_NIF_TERM error_tuple(ErlNifEnv*, const char*);
ERL_NIF_TERM make_output_binary(ErlNifEnv*, unsigned char*, size_t);

#endif // AR_NIF_H