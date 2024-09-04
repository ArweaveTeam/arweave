#include "ar_nif.h"
#include <string.h>

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

ERL_NIF_TERM error_tuple(ErlNifEnv* envPtr, const char* reason)
{
	ERL_NIF_TERM reasonTerm = enif_make_string(envPtr, reason, ERL_NIF_LATIN1);
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "error"), reasonTerm);
}

ERL_NIF_TERM make_output_binary(ErlNifEnv* envPtr, unsigned char *dataPtr, size_t size)
{
	ERL_NIF_TERM outputTerm;
	unsigned char *outputTermDataPtr;

	outputTermDataPtr = enif_make_new_binary(envPtr, size, &outputTerm);
	memcpy(outputTermDataPtr, dataPtr, size);
	return outputTerm;
}
