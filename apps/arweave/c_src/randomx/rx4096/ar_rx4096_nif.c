#include <string.h>
#include <openssl/sha.h>
#include <ar_nif.h>
#include "../randomx_long_with_entropy.h"
#include "../feistel_msgsize_key_cipher.h"

#include "../ar_randomx_impl.h"

const int PACKING_KEY_SIZE = 32;
const int MAX_CHUNK_SIZE = 256*1024;

static int rx4096_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info);
static ERL_NIF_TERM rx4096_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx4096_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx4096_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);

static int rx4096_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	return load(envPtr, priv, info);
}

static ERL_NIF_TERM rx4096_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return info_nif("rx4096", envPtr, argc, argv);
}

static ERL_NIF_TERM rx4096_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init_nif(envPtr, argc, argv);
}

static ERL_NIF_TERM rx4096_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_nif(envPtr, argc, argv);
}

static ErlNifFunc rx4096_funcs[] = {
	{"rx4096_info_nif", 1, rx4096_info_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_init_nif", 5, rx4096_init_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_hash_nif", 5, rx4096_hash_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
};

ERL_NIF_INIT(ar_rx4096_nif, rx4096_funcs, rx4096_load, NULL, NULL, NULL);
