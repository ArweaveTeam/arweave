#include <erl_nif.h>
#include <string.h>
#include "randomx.h"
#include "ar_mine_randomx.h"

ErlNifResourceType* stateType;

static ErlNifFunc nif_funcs[] = {
	{"init_fast_nif", 4, init_fast_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"init_light_nif", 3, init_light_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_fast_nif", 5, hash_fast_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_light_nif", 5, hash_light_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"release_state_nif", 1, release_state_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_mine_randomx, nif_funcs, load, NULL, NULL, NULL);

static int load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	int flags = ERL_NIF_RT_CREATE;
	stateType = enif_open_resource_type(envPtr, NULL, "state", state_dtor, flags, NULL);
	if (stateType == NULL) {
		return 1;
	} else {
		return 0;
	}
}

static void state_dtor(ErlNifEnv* envPtr, void* objPtr)
{
	struct state *statePtr = (struct state*) objPtr;

	release_randomx(statePtr);
	enif_rwlock_destroy(statePtr->lockPtr);
}

static void release_randomx(struct state *statePtr)
{
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

static ERL_NIF_TERM init_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM init_light_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init(envPtr, argc, argv, HASHING_MODE_LIGHT);
}

static ERL_NIF_TERM init(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[], hashing_mode mode)
{
	ErlNifBinary key;
	struct state *statePtr;
	ERL_NIF_TERM resource;
	unsigned int numWorkers;
	int jitEnabled, largePagesEnabled;
	randomx_flags flags;

	if (mode == HASHING_MODE_FAST && argc != 4) {
		return enif_make_badarg(envPtr);
	} else if (mode == HASHING_MODE_LIGHT && argc != 3) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[0], &key)) {
		return enif_make_badarg(envPtr);
	}
	if (mode == HASHING_MODE_FAST && !enif_get_uint(envPtr, argv[3], &numWorkers)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[1], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}

	statePtr = enif_alloc_resource(stateType, sizeof(struct state));
	statePtr->cachePtr = NULL;
	statePtr->datasetPtr = NULL;
	statePtr->isRandomxReleased = 0;

	statePtr->lockPtr = enif_rwlock_create("state_rw_lock");
	if (statePtr->lockPtr == NULL) {
		return init_failed(envPtr, statePtr, "enif_rwlock_create failed");
	}

	flags = RANDOMX_FLAG_DEFAULT;
	if (jitEnabled) {
		flags |= RANDOMX_FLAG_JIT;
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

static boolean init_dataset(randomx_dataset *datasetPtr, randomx_cache *cachePtr, unsigned int numWorkers)
{
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
		workerPtr->datasetInitItemCount = itemsPerThread + itemsRemainder * (i + 1 == numWorkers);
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
	}
	if (statePtr->cachePtr != NULL) {
		randomx_release_cache(statePtr->cachePtr);
	}
	if (statePtr->datasetPtr != NULL) {
		randomx_release_dataset(statePtr->datasetPtr);
	}
	enif_release_resource(statePtr);
	return error(envPtr, reason);
}

static ERL_NIF_TERM hash_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_nif(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM hash_light_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_nif(envPtr, argc, argv, HASHING_MODE_LIGHT);
}

static ERL_NIF_TERM hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[], hashing_mode hashingMode)
{
	randomx_vm *vmPtr = NULL;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	randomx_flags flags;
	char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary inputData;

	if (argc != 5) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
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

	enif_rwlock_rlock(statePtr->lockPtr);
	if (statePtr->isRandomxReleased != 0) {
		enif_rwlock_runlock(statePtr->lockPtr);
		return error(envPtr, "state has been released");
	}

	flags = RANDOMX_FLAG_DEFAULT;
	if (hashingMode == HASHING_MODE_FAST) {
		flags |= RANDOMX_FLAG_FULL_MEM;
	}
	if (hardwareAESEnabled) {
		flags |= RANDOMX_FLAG_HARD_AES;
	}
	if (jitEnabled) {
		flags |= RANDOMX_FLAG_JIT;
	}
	if (largePagesEnabled) {
		flags |= RANDOMX_FLAG_LARGE_PAGES;
	}
	vmPtr = randomx_create_vm(flags, statePtr->cachePtr, statePtr->datasetPtr);
	if (vmPtr == NULL) {
		enif_rwlock_runlock(statePtr->lockPtr);
		return error(envPtr, "randomx_create_vm failed");
	}
	randomx_calculate_hash(vmPtr, inputData.data, inputData.size, hashPtr);
	randomx_destroy_vm(vmPtr);
	enif_rwlock_runlock(statePtr->lockPtr);
	return ok_tuple(envPtr, make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE));
}

static ERL_NIF_TERM release_state_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	struct state* statePtr;

	if (argc != 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (enif_rwlock_tryrwlock(statePtr->lockPtr) != 0) {
		return error(envPtr, "failed to acquire the state lock, the state is being used");
	}
	release_randomx(statePtr);
	enif_rwlock_rwunlock(statePtr->lockPtr);
	return enif_make_atom(envPtr, "ok");
}

// Utility functions.

static ERL_NIF_TERM ok_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "ok"), term);
}

static ERL_NIF_TERM error_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "error"), term);
}

static ERL_NIF_TERM error(ErlNifEnv* envPtr, const char* reason)
{
	return error_tuple(envPtr, enif_make_string(envPtr, reason, ERL_NIF_LATIN1));
}

static ERL_NIF_TERM make_output_binary(ErlNifEnv* envPtr, char *dataPtr, size_t size)
{
	ERL_NIF_TERM outputTerm;
	unsigned char *outputTermDataPtr;

	outputTermDataPtr = enif_make_new_binary(envPtr, size, &outputTerm);
	memcpy(outputTermDataPtr, dataPtr, size);
	return outputTerm;
}
