#include "randomx.h"

typedef enum { FALSE, TRUE } boolean;

typedef enum {
	HASHING_MODE_FAST = 0,
	HASHING_MODE_LIGHT = 1,
} hashing_mode;

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

const int ARWEAVE_INPUT_DATA_SIZE = 48;

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
static ERL_NIF_TERM hash_nif(ErlNifEnv*, int, const ERL_NIF_TERM [], hashing_mode);

static ERL_NIF_TERM bulk_hash_fast_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM release_state_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);

static ERL_NIF_TERM ok_tuple(ErlNifEnv*, ERL_NIF_TERM);
static ERL_NIF_TERM ok_tuple4(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM);
static ERL_NIF_TERM error_tuple(ErlNifEnv*, ERL_NIF_TERM);
static ERL_NIF_TERM error(ErlNifEnv*, const char*);
static ERL_NIF_TERM make_output_binary(ErlNifEnv*, char*, size_t);
static int validate_hash(char[RANDOMX_HASH_SIZE], unsigned char[RANDOMX_HASH_SIZE]);
