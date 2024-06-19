#include "randomx.h"

typedef enum { FALSE, TRUE } boolean;

typedef enum {
	HASHING_MODE_FAST = 0,
	HASHING_MODE_LIGHT = 1,
} hashing_mode;

const int BIG_NUM_SIZE = 32;
typedef unsigned char bigInt[32];
const int MAX_CHUNK_SIZE = 256*1024;
const int PACKING_KEY_SIZE = 32;

struct workerThread {
	ErlNifTid threadId;
	ErlNifThreadOpts *optsPtr;
	randomx_cache *cachePtr;
	randomx_dataset *datasetPtr;
	unsigned long datasetInitStartItem;
	unsigned long datasetInitItemCount;
};

struct state {
	ErlNifRWLock*     lockPtr;
	int               isRandomxReleased;
	randomx_dataset*  datasetPtr;
	randomx_cache*    cachePtr;
};

struct wrap_randomx_vm {
	randomx_flags     flags;
	randomx_vm*       vmPtr;
};

const int ARWEAVE_INPUT_DATA_SIZE = 48;
const int ARWEAVE_HASH_SIZE = 48;
const int SPORA_SUBSPACES_COUNT = 1024;
const int SPORA_SEARCH_SPACE_SHARE = 10;

static int load(ErlNifEnv*, void**, ERL_NIF_TERM);
static void state_dtor(ErlNifEnv*, void*);
static void release_randomx(struct state*);

static ERL_NIF_TERM init_fast_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM init_light_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM init(ErlNifEnv*, int, const ERL_NIF_TERM [], hashing_mode);
static boolean init_dataset(randomx_dataset*, randomx_cache*, unsigned int);
static void *init_dataset_thread(void*);
static ERL_NIF_TERM init_failed(ErlNifEnv*, struct state*, const char*);

static ERL_NIF_TERM hash_fast_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM hash_light_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_hash_nif(ErlNifEnv*, int, const ERL_NIF_TERM [], hashing_mode);

static ERL_NIF_TERM bulk_hash_fast_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM hash_fast_verify_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM randomx_encrypt_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_decrypt_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_reencrypt_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM randomx_encrypt_composite_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_decrypt_composite_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_decrypt_composite_sub_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_reencrypt_legacy_to_composite_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM randomx_reencrypt_composite_to_composite_chunk_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM hash_fast_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM hash_light_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM randomx_hash_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[], hashing_mode hashingMode);
static ERL_NIF_TERM bulk_hash_fast_long_with_entropy_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM release_state_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM vdf_sha2_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static ERL_NIF_TERM solution_tuple(ErlNifEnv*, ERL_NIF_TERM);
static ERL_NIF_TERM ok_tuple(ErlNifEnv*, ERL_NIF_TERM);
static ERL_NIF_TERM ok_tuple2(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
static ERL_NIF_TERM error_tuple(ErlNifEnv*, ERL_NIF_TERM);
static ERL_NIF_TERM error(ErlNifEnv*, const char*);
static ERL_NIF_TERM make_output_binary(ErlNifEnv*, unsigned char*, size_t);
static int validate_hash(unsigned char[RANDOMX_HASH_SIZE], unsigned char[RANDOMX_HASH_SIZE]);

// From RandomX/src/jit_compiler.hpp
// needed for the JIT compiler to work on OpenBSD, NetBSD and Apple Silicon
#if defined(__OpenBSD__) || defined(__NetBSD__) || (defined(__APPLE__) && defined(__aarch64__))
#define RANDOMX_FORCE_SECURE
#endif
