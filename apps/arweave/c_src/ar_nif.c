#include <erl_nif.h>
#include "ar_nif.h"

// Utility functions.

ERL_NIF_TERM solution_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM hashTerm) {
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "true"), hashTerm);
}

ERL_NIF_TERM ok_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "ok"), term);
}

ERL_NIF_TERM ok_tuple2(ErlNifEnv* envPtr, ERL_NIF_TERM term1, ERL_NIF_TERM term2)
{
	return enif_make_tuple3(envPtr, enif_make_atom(envPtr, "ok"), term1, term2);
}

ERL_NIF_TERM error_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "error"), term);
}

ERL_NIF_TERM error(ErlNifEnv* envPtr, const char* reason)
{
	return error_tuple(envPtr, enif_make_string(envPtr, reason, ERL_NIF_LATIN1));
}

ERL_NIF_TERM make_output_binary(ErlNifEnv* envPtr, unsigned char *dataPtr, size_t size)
{
	ERL_NIF_TERM outputTerm;
	unsigned char *outputTermDataPtr;

	outputTermDataPtr = enif_make_new_binary(envPtr, size, &outputTerm);
	memcpy(outputTermDataPtr, dataPtr, size);
	return outputTerm;
}
