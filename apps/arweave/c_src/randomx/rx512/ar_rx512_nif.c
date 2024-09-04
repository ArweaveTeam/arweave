#include <string.h>
#include <erl_nif.h>
#include <openssl/sha.h>
#include "randomx.h"
#include <ar_nif.h>
#include "../ar_randomx.h"
#include "../randomx_long_with_entropy.h"
#include "../feistel_msgsize_key_cipher.h"

typedef enum { FALSE, TRUE } boolean;

typedef enum {
	HASHING_MODE_FAST = 0,
	HASHING_MODE_LIGHT = 1,
} hashing_mode;

extern const int MAX_CHUNK_SIZE;

struct workerThread {
	ErlNifTid threadId;
	ErlNifThreadOpts *optsPtr;
	randomx_cache *cachePtr;
	randomx_dataset *datasetPtr;
	unsigned long datasetInitStartItem;
	unsigned long datasetInitItemCount;
};

typedef struct {
	ErlNifRWLock*     lockPtr;
	int               isRandomxReleased;
	hashing_mode      mode;
	randomx_dataset*  datasetPtr;
	randomx_cache*    cachePtr;
} rx512_state;


ErlNifResourceType* rx512_stateType;

const int MAX_CHUNK_SIZE = 256*1024;

static void rx512_state_dtor(ErlNifEnv* envPtr, void* objPtr);
static void release_randomx(rx512_state *statePtr);
static boolean init_dataset(randomx_dataset *datasetPtr, randomx_cache *cachePtr, unsigned int numWorkers);
static void *init_dataset_thread(void *objPtr);
static randomx_vm* create_vm(rx512_state* statePtr,
		int fullMemEnabled, int jitEnabled, int largePagesEnabled, int hardwareAESEnabled,
		int* isRandomxReleased);
static void destroy_vm(rx512_state* statePtr, randomx_vm* vmPtr);	
static ERL_NIF_TERM init_failed(ErlNifEnv *envPtr, rx512_state *statePtr, const char* reason);
static ERL_NIF_TERM rx512_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_encrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_decrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_reencrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);


static void rx512_state_dtor(ErlNifEnv* envPtr, void* objPtr)
{
	fprintf(stderr, "rx512_state_dtor: called\n");
	rx512_state *statePtr = (rx512_state*) objPtr;

	release_randomx(statePtr);
	if (statePtr->lockPtr != NULL) {
		enif_rwlock_destroy(statePtr->lockPtr);
		statePtr->lockPtr = NULL;
	}
}

static void release_randomx(rx512_state *statePtr)
{
	fprintf(stderr, "release_randomx: called\n");
	if (statePtr->datasetPtr != NULL) {
		randomx_release_dataset(statePtr->datasetPtr);
		statePtr->datasetPtr = NULL;
	}
	if (statePtr->cachePtr != NULL) {
		randomx_release_cache(statePtr->cachePtr);
		statePtr->cachePtr = NULL;
	}
	statePtr->isRandomxReleased = 1;
}

static ERL_NIF_TERM rx512_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	fprintf(stderr, "rx512_info_nif: called\n");
	rx512_state* statePtr;
	unsigned int datasetSize;
	hashing_mode hashingMode;
	ERL_NIF_TERM hashingModeTerm;

	if (argc != 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], rx512_stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read rx512_state");
	}

	hashingMode = statePtr->mode;

	if (hashingMode == HASHING_MODE_FAST) {
		if (statePtr->datasetPtr == NULL) {
			return error(envPtr, "dataset is not initialized for fast hashing mode");
		}
		if (statePtr->cachePtr != NULL) {
			return error(envPtr, "cache is initialized for fast hashing mode");
		}
		datasetSize = randomx_dataset_item_count();
		hashingModeTerm = enif_make_atom(envPtr, "fast");
	} else if (hashingMode == HASHING_MODE_LIGHT) {
		if (statePtr->datasetPtr != NULL) {
			return error(envPtr, "dataset is initialized for light hashing mode");
		}
		if (statePtr->cachePtr == NULL) {
			return error(envPtr, "cache is not initialized for light hashing mode");
		}
		datasetSize = 0;
		hashingModeTerm = enif_make_atom(envPtr, "light");
	} else {
		return error(envPtr, "invalid hashing mode");
	}


	return ok_tuple2(envPtr, hashingModeTerm, enif_make_uint(envPtr, datasetSize));
}


static ERL_NIF_TERM rx512_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	fprintf(stderr, "rx512_init_nif: called\n");
	ErlNifBinary key;
	hashing_mode mode;
	rx512_state *statePtr;
	ERL_NIF_TERM resource;
	unsigned int numWorkers;
	int jitEnabled, largePagesEnabled;
	randomx_flags flags;

	if (!enif_inspect_binary(envPtr, argv[0], &key)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[1], &mode)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[4], &numWorkers)) {
		return enif_make_badarg(envPtr);
	}

	statePtr = enif_alloc_resource(rx512_stateType, sizeof(rx512_state));
	statePtr->cachePtr = NULL;
	statePtr->datasetPtr = NULL;
	statePtr->isRandomxReleased = 0;
	statePtr->mode = mode;

	statePtr->lockPtr = enif_rwlock_create("state_rw_lock");
	if (statePtr->lockPtr == NULL) {
		return init_failed(envPtr, statePtr, "enif_rwlock_create failed");
	}

	flags = RANDOMX_FLAG_DEFAULT;
	if (jitEnabled) {
		flags |= RANDOMX_FLAG_JIT;
#ifdef RANDOMX_FORCE_SECURE
		flags |= RANDOMX_FLAG_SECURE;
#endif
	}
	if (largePagesEnabled) {
		flags |= RANDOMX_FLAG_LARGE_PAGES;
	}

	statePtr->cachePtr = randomx_alloc_cache(flags);
	if (statePtr->cachePtr == NULL) {
		return init_failed(envPtr, statePtr, "randomx_alloc_cache failed");
	}

	randomx_init_cache(
		statePtr->cachePtr,
		key.data,
		key.size);

	if (mode == HASHING_MODE_FAST) {
		statePtr->datasetPtr = randomx_alloc_dataset(flags);
		if (statePtr->datasetPtr == NULL) {
			return init_failed(envPtr, statePtr, "randomx_alloc_dataset failed");
		}
		if (!init_dataset(statePtr->datasetPtr, statePtr->cachePtr, numWorkers)) {
			return init_failed(envPtr, statePtr, "init_dataset failed");
		}
		randomx_release_cache(statePtr->cachePtr);
		statePtr->cachePtr = NULL;
	} else {
		statePtr->datasetPtr = NULL;
	}

	resource = enif_make_resource(envPtr, statePtr);
	enif_release_resource(statePtr);

	return ok_tuple(envPtr, resource);
}

static boolean init_dataset(
	randomx_dataset *datasetPtr,
	randomx_cache *cachePtr,
	unsigned int numWorkers
) {
	fprintf(stderr, "init_dataset: called\n");
	struct workerThread **workerPtrPtr;
	struct workerThread *workerPtr;
	unsigned long itemsPerThread;
	unsigned long itemsRemainder;
	unsigned long startItem;
	boolean anyThreadFailed;

	workerPtrPtr = enif_alloc(sizeof(struct workerThread *) * numWorkers);
	itemsPerThread = randomx_dataset_item_count() / numWorkers;
	itemsRemainder = randomx_dataset_item_count() % numWorkers;
	startItem = 0;
	for (int i = 0; i < numWorkers; i++) {
		workerPtrPtr[i] = enif_alloc(sizeof(struct workerThread));
		workerPtr = workerPtrPtr[i];

		workerPtr->cachePtr = cachePtr;
		workerPtr->datasetPtr = datasetPtr;

		workerPtr->datasetInitStartItem = startItem;
		if (i + 1 == numWorkers) {
			workerPtr->datasetInitItemCount = itemsPerThread + itemsRemainder;
		} else {
			workerPtr->datasetInitItemCount = itemsPerThread;
		}
		startItem += workerPtr->datasetInitItemCount;
		workerPtr->optsPtr = enif_thread_opts_create("init_fast_worker");
		if (0 != enif_thread_create(
				"init_dataset_worker",
				&(workerPtr->threadId),
				&init_dataset_thread,
				workerPtr,
				workerPtr->optsPtr))
		{
			enif_thread_opts_destroy(workerPtr->optsPtr);
			enif_free(workerPtrPtr[i]);
			workerPtrPtr[i] = NULL;
		}
	}
	anyThreadFailed = FALSE;
	for (int i = 0; i < numWorkers; i++) {
		workerPtr = workerPtrPtr[i];
		if (workerPtr == NULL) {
			anyThreadFailed = TRUE;
		} else if (0 != enif_thread_join(workerPtr->threadId, NULL)) {
			anyThreadFailed = TRUE;
		}
		if (workerPtr != NULL) {
			enif_thread_opts_destroy(workerPtr->optsPtr);
			enif_free(workerPtr);
		}
	}
	enif_free(workerPtrPtr);
	return !anyThreadFailed;
}

static void *init_dataset_thread(void *objPtr)
{
	fprintf(stderr, "init_dataset_thread: called\n");
	struct workerThread *workerPtr = (struct workerThread*) objPtr;
	randomx_init_dataset(
		workerPtr->datasetPtr,
		workerPtr->cachePtr,
		workerPtr->datasetInitStartItem,
		workerPtr->datasetInitItemCount);
	return NULL;
}

static ERL_NIF_TERM init_failed(ErlNifEnv *envPtr, rx512_state *statePtr, const char* reason)
{
	fprintf(stderr, "init_failed: called\n");
	if (statePtr->lockPtr != NULL) {
		enif_rwlock_destroy(statePtr->lockPtr);
		statePtr->lockPtr = NULL;
	}
	if (statePtr->cachePtr != NULL) {
		randomx_release_cache(statePtr->cachePtr);
		statePtr->cachePtr = NULL;
	}
	if (statePtr->datasetPtr != NULL) {
		randomx_release_dataset(statePtr->datasetPtr);
		statePtr->datasetPtr = NULL;
	}
	enif_release_resource(statePtr);
	return error(envPtr, reason);
}

static randomx_vm* create_vm(rx512_state* statePtr,
		int fullMemEnabled, int jitEnabled, int largePagesEnabled, int hardwareAESEnabled,
		int* isRandomxReleased) {
	fprintf(stderr, "create_vm: called\n");
	enif_rwlock_rlock(statePtr->lockPtr);
	*isRandomxReleased = statePtr->isRandomxReleased;
	if (statePtr->isRandomxReleased != 0) {
		enif_rwlock_runlock(statePtr->lockPtr);
		return NULL;
	}

	randomx_flags flags = RANDOMX_FLAG_DEFAULT;
	if (fullMemEnabled) {
		flags |= RANDOMX_FLAG_FULL_MEM;
	}
	if (hardwareAESEnabled) {
		flags |= RANDOMX_FLAG_HARD_AES;
	}
	if (jitEnabled) {
		flags |= RANDOMX_FLAG_JIT;
#ifdef RANDOMX_FORCE_SECURE
		flags |= RANDOMX_FLAG_SECURE;
#endif
	}
	if (largePagesEnabled) {
		flags |= RANDOMX_FLAG_LARGE_PAGES;
	}

	randomx_vm *vmPtr = randomx_create_vm(flags, statePtr->cachePtr, statePtr->datasetPtr);
	if (vmPtr == NULL) {
		enif_rwlock_runlock(statePtr->lockPtr);
		return NULL;
	}
	return vmPtr;
}

static void destroy_vm(rx512_state* statePtr, randomx_vm* vmPtr) {
	fprintf(stderr, "destroy_vm: called\n");
	randomx_destroy_vm(vmPtr);
	enif_rwlock_runlock(statePtr->lockPtr);
}

static ERL_NIF_TERM rx512_hash_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	fprintf(stderr, "rx512_hash_nif: called\n");
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	rx512_state* statePtr;
	ErlNifBinary inputData;

	fprintf(stderr, "A\n");

	if (argc != 5) {
		return enif_make_badarg(envPtr);
	}
	fprintf(stderr, "B\n");
	if (!enif_get_resource(envPtr, argv[0], rx512_stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read rx512_state");
	}
	fprintf(stderr, "C\n");
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	fprintf(stderr, "D\n");
	if (!enif_get_int(envPtr, argv[2], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	fprintf(stderr, "E\n");
	if (!enif_get_int(envPtr, argv[3], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	fprintf(stderr, "F\n");
	if (!enif_get_int(envPtr, argv[4], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	fprintf(stderr, "rx512_hash_nif: jitEnabled: %d, largePagesEnabled: %d, hardwareAESEnabled: %d\n", jitEnabled, largePagesEnabled, hardwareAESEnabled);

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST), jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "rx512_state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	randomx_calculate_hash(vmPtr, inputData.data, inputData.size, hashPtr);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE));
}


static ERL_NIF_TERM decrypt_chunk(ErlNifEnv* envPtr,
		randomx_vm *machine, const unsigned char *input, const size_t inputSize,
		const unsigned char *inChunk, const size_t inChunkSize,
		unsigned char* outChunk, const size_t outChunkSize,
		const int randomxProgramCount) {
	fprintf(stderr, "decrypt_chunk: called\n");
	randomx_decrypt_chunk(
		machine, input, inputSize, inChunk, inChunkSize, outChunk, randomxProgramCount);
	return make_output_binary(envPtr, outChunk, outChunkSize);
}

static ERL_NIF_TERM encrypt_chunk(ErlNifEnv* envPtr,
		randomx_vm *machine, const unsigned char *input, const size_t inputSize,
		const unsigned char *inChunk, const size_t inChunkSize,
		const int randomxProgramCount) {
	fprintf(stderr, "encrypt_chunk: called\n");
	ERL_NIF_TERM encryptedChunkTerm;
	unsigned char* encryptedChunk = enif_make_new_binary(
										envPtr, MAX_CHUNK_SIZE, &encryptedChunkTerm);

	if (inChunkSize < MAX_CHUNK_SIZE) {
		unsigned char *paddedInChunk = (unsigned char*)malloc(MAX_CHUNK_SIZE);
		memset(paddedInChunk, 0, MAX_CHUNK_SIZE);
		memcpy(paddedInChunk, inChunk, inChunkSize);
		randomx_encrypt_chunk(
			machine, input, inputSize, paddedInChunk, MAX_CHUNK_SIZE,
			encryptedChunk, randomxProgramCount);
		free(paddedInChunk);
	} else {
		randomx_encrypt_chunk(
			machine, input, inputSize, inChunk, inChunkSize,
			encryptedChunk, randomxProgramCount);
	}

	return encryptedChunkTerm;
}

static ERL_NIF_TERM rx512_encrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	fprintf(stderr, "rx512_encrypt_chunk_nif: called\n");
	int randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	rx512_state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 7) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], rx512_stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read rx512_state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk) ||
		inputChunk.size == 0 ||
		inputChunk.size > MAX_CHUNK_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[6], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
		jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "rx512_state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	ERL_NIF_TERM outChunkTerm = encrypt_chunk(envPtr, vmPtr,
		inputData.data, inputData.size, inputChunk.data, inputChunk.size, randomxRoundCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, outChunkTerm);
}

static ERL_NIF_TERM rx512_decrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	fprintf(stderr, "rx512_decrypt_chunk_nif: called\n");
	int outChunkLen, randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	rx512_state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 8) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], rx512_stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read rx512_state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &outChunkLen)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[6], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[7], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
		jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "rx512_state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	// NOTE. Because randomx_decrypt_chunk will unpack padding too, decrypt always uses the
	// full 256KB chunk size. We'll then truncate the output to the correct feistel-padded
	// outChunkSize.
	unsigned char outChunk[MAX_CHUNK_SIZE];
	ERL_NIF_TERM decryptedChunkTerm = decrypt_chunk(envPtr, vmPtr,
		inputData.data, inputData.size, inputChunk.data, inputChunk.size,
		outChunk, outChunkLen, randomxRoundCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, decryptedChunkTerm);
}

static ERL_NIF_TERM rx512_reencrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	fprintf(stderr, "rx512_reencrypt_chunk_nif: called\n");
	int chunkSize, decryptRandomxRoundCount, encryptRandomxRoundCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	rx512_state* statePtr;
	ErlNifBinary decryptKey;
	ErlNifBinary encryptKey;
	ErlNifBinary inputChunk;

	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], rx512_stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read rx512_state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &decryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &encryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[3], &inputChunk) || inputChunk.size == 0) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &chunkSize)  ||
		chunkSize == 0 ||
		chunkSize > MAX_CHUNK_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &decryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[6], &encryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[7], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[9], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
		jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "rx512_state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	// NOTE. Because randomx_decrypt_chunk will unpack padding too, decrypt always uses the
	// full 256KB chunk size. We'll then truncate the output to the correct feistel-padded
	// outChunkSize.
	unsigned char decryptedChunk[MAX_CHUNK_SIZE];
	ERL_NIF_TERM decryptedChunkTerm = decrypt_chunk(envPtr, vmPtr,
		decryptKey.data, decryptKey.size, inputChunk.data, inputChunk.size,
		decryptedChunk, chunkSize, decryptRandomxRoundCount);

	ERL_NIF_TERM reencryptedChunkTerm = encrypt_chunk(envPtr, vmPtr,
		encryptKey.data, encryptKey.size, decryptedChunk, chunkSize, encryptRandomxRoundCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple2(envPtr, reencryptedChunkTerm, decryptedChunkTerm);
}

static ErlNifFunc rx512_funcs[] = {
	{"rx512_info_nif", 1, rx512_info_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx512_init_nif", 5, rx512_init_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx512_hash_nif", 5, rx512_hash_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx512_encrypt_chunk_nif", 7, rx512_encrypt_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx512_decrypt_chunk_nif", 8, rx512_decrypt_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx512_reencrypt_chunk_nif", 10, rx512_reencrypt_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

static int rx512_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	fprintf(stderr, "rx512_load: called\n");
	int flags = ERL_NIF_RT_CREATE;
	rx512_stateType = enif_open_resource_type(envPtr, NULL, "rx512_state", rx512_state_dtor, flags, NULL);
	if (rx512_stateType == NULL) {
		return 1;
	}

	return 0;
}

ERL_NIF_INIT(ar_rx512_nif, rx512_funcs, rx512_load, NULL, NULL, NULL);

