#ifndef AR_RANDOMX_IMPL_H
#define AR_RANDOMX_IMPL_H

// Thif file includes the full definitions of any function that is shared between the
// rx512 and rx4096 shared libraries. Although ugly this was the only way I could get
// everything to work without causing symbol conflicts or seg faults once the two .so's
// are loaded into arweave and the NIFs registered. There may be a better way!

#include <erl_nif.h>
#include <randomx.h>

// From RandomX/src/jit_compiler.hpp
// needed for the JIT compiler to work on OpenBSD, NetBSD and Apple Silicon
#if defined(__OpenBSD__) || defined(__NetBSD__) || (defined(__APPLE__) && defined(__aarch64__))
#define RANDOMX_FORCE_SECURE
#endif

typedef enum { FALSE, TRUE } boolean;

struct workerThread {
	ErlNifTid threadId;
	ErlNifThreadOpts *optsPtr;
	randomx_cache *cachePtr;
	randomx_dataset *datasetPtr;
	unsigned long datasetInitStartItem;
	unsigned long datasetInitItemCount;
};

typedef enum {
	HASHING_MODE_FAST = 0,
	HASHING_MODE_LIGHT = 1,
} hashing_mode;

struct state {
	ErlNifRWLock*     lockPtr;
	int               isRandomxReleased;
	hashing_mode      mode;
	randomx_dataset*  datasetPtr;
	randomx_cache*    cachePtr;
};

ErlNifResourceType* stateType;

static ERL_NIF_TERM init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM info_nif(const char* rxSize, ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static int load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info);
static void state_dtor(ErlNifEnv* envPtr, void* objPtr);
static boolean init_dataset(
	randomx_dataset *datasetPtr,
	randomx_cache *cachePtr,
	unsigned int numWorkers
);
static void *init_dataset_thread(void *objPtr);
static ERL_NIF_TERM init_failed(ErlNifEnv *envPtr, struct state *statePtr, const char* reason);
static randomx_vm* create_vm(struct state* statePtr,
		int fullMemEnabled, int jitEnabled, int largePagesEnabled, int hardwareAESEnabled,
		int* isRandomxReleased);
static void destroy_vm(struct state* statePtr, randomx_vm* vmPtr);

static ERL_NIF_TERM init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary key;
	hashing_mode mode;
	struct state *statePtr;
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

	statePtr = enif_alloc_resource(stateType, sizeof(struct state));
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

static ERL_NIF_TERM info_nif(
    const char* rxSize, ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	struct state* statePtr;
	unsigned int datasetSize;
	unsigned int scratchpadSize;
	hashing_mode hashingMode;
	ERL_NIF_TERM hashingModeTerm;

	if (argc != 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error_tuple(envPtr, "failed to read state");
	}

	hashingMode = statePtr->mode;

	if (hashingMode == HASHING_MODE_FAST) {
		if (statePtr->datasetPtr == NULL) {
			return error_tuple(envPtr, "dataset is not initialized for fast hashing mode");
		}
		if (statePtr->cachePtr != NULL) {
			return error_tuple(envPtr, "cache is initialized for fast hashing mode");
		}
		hashingModeTerm = enif_make_atom(envPtr, "fast");
		datasetSize = randomx_dataset_item_count();
		scratchpadSize = randomx_get_scratchpad_size();
	} else if (hashingMode == HASHING_MODE_LIGHT) {
		if (statePtr->datasetPtr != NULL) {
			return error_tuple(envPtr, "dataset is initialized for light hashing mode");
		}
		if (statePtr->cachePtr == NULL) {
			return error_tuple(envPtr, "cache is not initialized for light hashing mode");
		}
		hashingModeTerm = enif_make_atom(envPtr, "light");
		datasetSize = 0;
		scratchpadSize = randomx_get_scratchpad_size();
	} else {
		return error_tuple(envPtr, "invalid hashing mode");
	}

	ERL_NIF_TERM infoTerm = enif_make_tuple4(envPtr,
		enif_make_atom(envPtr, rxSize),
		hashingModeTerm,
		enif_make_uint(envPtr, datasetSize),
		enif_make_uint(envPtr, scratchpadSize));
	return ok_tuple(envPtr, infoTerm);
}


static ERL_NIF_TERM hash_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary inputData;

	if (argc != 5) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error_tuple(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST), jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
	}

	randomx_calculate_hash(vmPtr, inputData.data, inputData.size, hashPtr);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE));
}

static int load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	int flags = ERL_NIF_RT_CREATE;
	stateType = enif_open_resource_type(envPtr, NULL, "state", state_dtor, flags, NULL);
	if (stateType == NULL) {
		return 1;
	}

	return 0;
}

static void state_dtor(ErlNifEnv* envPtr, void* objPtr)
{
	struct state *statePtr = (struct state*) objPtr;

    if (statePtr->datasetPtr != NULL) {
		randomx_release_dataset(statePtr->datasetPtr);
		statePtr->datasetPtr = NULL;
	}
	if (statePtr->cachePtr != NULL) {
		randomx_release_cache(statePtr->cachePtr);
		statePtr->cachePtr = NULL;
	}
	statePtr->isRandomxReleased = 1;

	if (statePtr->lockPtr != NULL) {
		enif_rwlock_destroy(statePtr->lockPtr);
		statePtr->lockPtr = NULL;
	}
}

static boolean init_dataset(
	randomx_dataset *datasetPtr,
	randomx_cache *cachePtr,
	unsigned int numWorkers
) {
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
	struct workerThread *workerPtr = (struct workerThread*) objPtr;
	randomx_init_dataset(
		workerPtr->datasetPtr,
		workerPtr->cachePtr,
		workerPtr->datasetInitStartItem,
		workerPtr->datasetInitItemCount);
	return NULL;
}

static ERL_NIF_TERM init_failed(ErlNifEnv *envPtr, struct state *statePtr, const char* reason)
{
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
	return error_tuple(envPtr, reason);
}

static randomx_vm* create_vm(struct state* statePtr,
		int fullMemEnabled, int jitEnabled, int largePagesEnabled, int hardwareAESEnabled,
		int* isRandomxReleased) {
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

static void destroy_vm(struct state* statePtr, randomx_vm* vmPtr) {
	randomx_destroy_vm(vmPtr);
	enif_rwlock_runlock(statePtr->lockPtr);
}

#endif