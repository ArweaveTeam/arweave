#include <erl_nif.h>
#include <string.h>
#include "randomx.h"

typedef enum {
	HASHING_MODE_FAST = 0,
	HASHING_MODE_LIGHT = 1,
} hashing_mode;

struct init_dataset_thread_state {
	ErlNifTid           tid;
	ErlNifThreadOpts*   optsPtr;
	randomx_dataset     *datasetPtr;
	randomx_cache       *cachePtr;
	size_t              startItem;
	size_t              count;
};

struct init_thread_state {
	ErlNifTid           tid;
	ErlNifThreadOpts*   optsPtr;
	ErlNifPid           parentPID;
	ErlNifEnv*          envPtr;
	ERL_NIF_TERM        ref;
	hashing_mode        hashingMode;
	ERL_NIF_TERM        vmState;
	// init-specific fields
	ErlNifBinary        key;
	size_t              threadCount;
};

struct hash_thread_state {
	ErlNifTid           tid;
	ErlNifThreadOpts*   optsPtr;
	ErlNifPid           parentPID;
	ErlNifEnv*          envPtr;
	ERL_NIF_TERM        ref;
	hashing_mode        hashingMode;
	ERL_NIF_TERM        vmState;
	// hash-specific fields
	ErlNifBinary        inputData;
};

struct release_thread_state {
	ErlNifTid           tid;
	ErlNifThreadOpts*   optsPtr;
	ErlNifPid           parentPID;
	ErlNifEnv*          envPtr;
	ERL_NIF_TERM        ref;
	ERL_NIF_TERM        vmState;
};

struct state {
	ErlNifRWLock*     lockPtr;
	int               isReleased;
	randomx_dataset*  datasetPtr;
	randomx_cache*    cachePtr;
};

ErlNifResourceType* stateType;
ErlNifResourceType* initThreadStateType;
ErlNifResourceType* hashThreadStateType;
ErlNifResourceType* releaseThreadStateType;

static ERL_NIF_TERM ok_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "ok"), term);
}

static ERL_NIF_TERM ok_tuple3(ErlNifEnv* envPtr, ERL_NIF_TERM term1, ERL_NIF_TERM term2)
{
	return enif_make_tuple3(envPtr, enif_make_atom(envPtr, "ok"), term1, term2);
}

static ERL_NIF_TERM error_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "error"), term);
}

static ERL_NIF_TERM error_tuple3(ErlNifEnv* envPtr, ERL_NIF_TERM term, const char* reason)
{
	return enif_make_tuple3(
		envPtr,
		enif_make_atom(envPtr, "error"),
		term,
		enif_make_string(envPtr, reason, ERL_NIF_LATIN1)
	);
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

static void *init_dataset_run(void *obj)
{
	struct init_dataset_thread_state *threadStatePtr = (struct init_dataset_thread_state*) obj;
	randomx_init_dataset(
		threadStatePtr->datasetPtr,
		threadStatePtr->cachePtr,
		threadStatePtr->startItem,
		threadStatePtr->count
	);
	return NULL;
}

static int init_dataset(struct init_thread_state* threadStatePtr, struct state* statePtr)
{
	size_t threadCount = threadStatePtr->threadCount;
	if (threadCount < 1) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"thread count must be 1 or bigger"
			)
		);
		return 1;
	}

	size_t datasetItemCount = randomx_dataset_item_count();
	if (threadCount > 1) {
		struct init_dataset_thread_state** threads = enif_alloc(
			sizeof(struct init_dataset_thread_state*) * threadCount
		);
		size_t perThread = datasetItemCount / threadCount;
		size_t remainder = datasetItemCount % threadCount;
		size_t startItem = 0;
		int status = 0;
		for (int i = 0; i < threadCount; i++) {
			size_t count = perThread + (i == threadCount - 1 ? remainder : 0);
			struct init_dataset_thread_state* threadPtr = enif_alloc(
				sizeof(struct init_dataset_thread_state)
			);
			threadPtr->optsPtr = enif_thread_opts_create("state");
			threadPtr->datasetPtr = statePtr->datasetPtr;
			threadPtr->cachePtr = statePtr->cachePtr;
			threadPtr->startItem = startItem;
			threadPtr->count = count;
			int threadStatus = enif_thread_create(
				"init_dataset_thread",
				&(threadPtr->tid),
				&init_dataset_run,
				threadPtr,
				threadPtr->optsPtr
			);
			status = status || threadStatus;
			if (threadStatus == 0) {
				threads[i] = threadPtr;
			}
			startItem += count;
		}
		for (int i = 0; i < threadCount; i++) {
			if (threads[i] == NULL) {
				continue;
			}
			enif_thread_join(threads[i]->tid, NULL);
			enif_free(threads[i]);
		}
		enif_free(threads);
		if (status != 0) {
			enif_send(
				NULL,
				&(threadStatePtr->parentPID),
				threadStatePtr->envPtr,
				error_tuple3(
					threadStatePtr->envPtr,
					threadStatePtr->ref,
					"failed to initialise one or more of the dataset initialisation threads"
				)
			);
			return 1;
		}
	}
	else {
		randomx_init_dataset(statePtr->datasetPtr, statePtr->cachePtr, 0, datasetItemCount);

	}
	randomx_release_cache(statePtr->cachePtr);
	statePtr->cachePtr = NULL;
	return 0;
}

static void *init_run(void* obj)
{
	struct init_thread_state *threadStatePtr = (struct init_thread_state*) obj;
	struct state* statePtr;
	if (!enif_get_resource(threadStatePtr->envPtr, threadStatePtr->vmState, stateType, (void**) &statePtr)) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"enif_get_resource (state) for init_run failed"
			)
		);
		goto init_run_cleanup;

	}
	enif_keep_resource(statePtr);
	enif_rwlock_rlock(statePtr->lockPtr);
	if (statePtr->isReleased != 0) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"state has been released"
			)
		);
		goto init_run_unlock_state;
	}

	if (statePtr->cachePtr == NULL) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"randomx_alloc_cache failed"
			)
		);
		goto init_run_unlock_state;
	}
	randomx_init_cache(
		statePtr->cachePtr,
		threadStatePtr->key.data,
		threadStatePtr->key.size
	);
	if (threadStatePtr->hashingMode == HASHING_MODE_FAST) {
		if (init_dataset(threadStatePtr, statePtr) != 0) {
			goto init_run_unlock_state;
		}
	}
	enif_send(
		NULL,
		&(threadStatePtr->parentPID),
		threadStatePtr->envPtr,
		ok_tuple(
			threadStatePtr->envPtr,
			threadStatePtr->ref
		)
	);
init_run_unlock_state:
	enif_rwlock_runlock(statePtr->lockPtr);
init_run_cleanup:
	enif_release_resource(statePtr);
	return NULL;
}

static ERL_NIF_TERM init_thread_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[], hashing_mode hashingMode)
{
	struct init_thread_state *threadStatePtr = enif_alloc_resource(
		initThreadStateType,
		sizeof(struct init_thread_state)
	);
	struct state* statePtr = enif_alloc_resource(stateType, sizeof(struct state));
	statePtr->lockPtr = enif_rwlock_create("state_rw_lock");
	statePtr->isReleased = 0;
	unsigned allocCacheFlags = RANDOMX_FLAG_DEFAULT;
	statePtr->cachePtr = randomx_alloc_cache(allocCacheFlags);
	unsigned allocDatasetFlags = RANDOMX_FLAG_DEFAULT;
	statePtr->datasetPtr = randomx_alloc_dataset(allocDatasetFlags);
	ERL_NIF_TERM stateTerm = enif_make_resource(envPtr, statePtr);
	threadStatePtr->optsPtr = enif_thread_opts_create("state");
	threadStatePtr->envPtr = enif_alloc_env();
	threadStatePtr->vmState = enif_make_copy(threadStatePtr->envPtr, stateTerm);
	threadStatePtr->hashingMode = hashingMode;

	int expectedArgs = 3;
	if (hashingMode == HASHING_MODE_FAST) {
		expectedArgs = 4;
	}
	if (argc != expectedArgs) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_local_pid(envPtr, argv[0], &(threadStatePtr->parentPID))) {
		return enif_make_badarg(envPtr);
	}
	threadStatePtr->ref = enif_make_copy(threadStatePtr->envPtr, argv[1]);
	if (!enif_inspect_binary(envPtr, argv[2], &(threadStatePtr->key))) {
		return enif_make_badarg(envPtr);
	}
	if (hashingMode == HASHING_MODE_FAST) {
		if (!enif_get_ulong(envPtr, argv[3], &(threadStatePtr->threadCount))) {
			return enif_make_badarg(envPtr);
		}
	}

	int status = enif_thread_create(
		"init_thread",
		&(threadStatePtr->tid),
		&init_run,
		threadStatePtr,
		threadStatePtr->optsPtr
	);
	if (status != 0) {
		return error(envPtr, "enif_thread_create for init_thread failed");
	}
	ERL_NIF_TERM threadStateTerm = enif_make_resource(envPtr, threadStatePtr);
	enif_release_resource(threadStatePtr);
	enif_release_resource(statePtr);
	return ok_tuple3(envPtr, stateTerm, threadStateTerm);
}

static ERL_NIF_TERM init_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init_thread_nif(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM init_light_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init_thread_nif(envPtr, argc, argv, HASHING_MODE_LIGHT);
}

static void *hash_run(void* obj)
{
	struct hash_thread_state *threadStatePtr = (struct hash_thread_state*) obj;
	randomx_vm *vmPtr = NULL;
	struct state* statePtr;
	if (!enif_get_resource(threadStatePtr->envPtr, threadStatePtr->vmState, stateType, (void**) &statePtr)) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"enif_get_resource (state) for hash_run failed"
			)
		);
		goto hash_run_cleanup;

	}
	enif_keep_resource(statePtr);
	enif_rwlock_rlock(statePtr->lockPtr);
	if (statePtr->isReleased != 0) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"state has been released"
			)
		);
		goto hash_run_unlock_state;
	}

	unsigned flags = RANDOMX_FLAG_DEFAULT;
	char hashPtr[RANDOMX_HASH_SIZE];

	if (threadStatePtr->hashingMode == HASHING_MODE_FAST) {
		flags |= RANDOMX_FLAG_FULL_MEM;
	}
	vmPtr = randomx_create_vm(flags, statePtr->cachePtr, statePtr->datasetPtr);
	if (vmPtr == NULL) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"randomx_create_vm failed"
			)
		);
		goto hash_run_unlock_state;
	}
	randomx_calculate_hash(
		vmPtr,
		threadStatePtr->inputData.data,
		threadStatePtr->inputData.size,
		hashPtr
	);
	enif_send(
		NULL,
		&(threadStatePtr->parentPID),
		threadStatePtr->envPtr,
		ok_tuple3(
			threadStatePtr->envPtr,
			threadStatePtr->ref,
			make_output_binary(threadStatePtr->envPtr, hashPtr, RANDOMX_HASH_SIZE)
		)
	);
hash_run_unlock_state:
	enif_rwlock_runlock(statePtr->lockPtr);
hash_run_cleanup:
	if (vmPtr != NULL) {
		randomx_destroy_vm(vmPtr);
	}
	enif_release_resource(statePtr);
	return NULL;
}

static ERL_NIF_TERM hash_thread_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[], hashing_mode hashingMode)
{
	struct hash_thread_state *threadStatePtr = enif_alloc_resource(
		hashThreadStateType,
		sizeof(struct hash_thread_state)
	);
	threadStatePtr->optsPtr = enif_thread_opts_create("state");
	threadStatePtr->envPtr = enif_alloc_env();
	threadStatePtr->hashingMode = hashingMode;

	if (argc != 4) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_local_pid(envPtr, argv[0], &(threadStatePtr->parentPID))) {
		return enif_make_badarg(envPtr);
	}
	threadStatePtr->ref = enif_make_copy(threadStatePtr->envPtr, argv[1]);
	threadStatePtr->vmState = enif_make_copy(threadStatePtr->envPtr, argv[2]);
	if (!enif_inspect_binary(envPtr, argv[3], &(threadStatePtr->inputData))) {
		return enif_make_badarg(envPtr);
	}

	int status = enif_thread_create(
		"hash_thread",
		&(threadStatePtr->tid),
		&hash_run,
		threadStatePtr,
		threadStatePtr->optsPtr
	);
	if (status != 0) {
		return error(envPtr, "enif_thread_create for hash_thread failed");
	}
	ERL_NIF_TERM threadStateTerm = enif_make_resource(envPtr, threadStatePtr);
	enif_release_resource(threadStatePtr);
	return ok_tuple(envPtr, threadStateTerm);
}

static ERL_NIF_TERM hash_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_thread_nif(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM hash_light_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_thread_nif(envPtr, argc, argv, HASHING_MODE_LIGHT);
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
}

static void *release_state_run(void* obj)
{
	struct release_thread_state *threadStatePtr = (struct release_thread_state*) obj;
	struct state* statePtr;
	if (!enif_get_resource(threadStatePtr->envPtr, threadStatePtr->vmState, stateType, (void**) &statePtr)) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"enif_get_resource (state) for release_state_run failed"
			)
		);
		return NULL;
	}
	enif_keep_resource(statePtr);

	if (enif_rwlock_tryrwlock(statePtr->lockPtr) != 0) {
		enif_send(
			NULL,
			&(threadStatePtr->parentPID),
			threadStatePtr->envPtr,
			error_tuple3(
				threadStatePtr->envPtr,
				threadStatePtr->ref,
				"failed to acquire the state lock, the state is being used"
			)
		);
		goto release_state_run_cleanup;
	}
	state_dtor(threadStatePtr->envPtr, statePtr);
	statePtr->isReleased = 1;
	enif_send(
		NULL,
		&(threadStatePtr->parentPID),
		threadStatePtr->envPtr,
		ok_tuple(threadStatePtr->envPtr, threadStatePtr->ref)
	);
	enif_rwlock_rwunlock(statePtr->lockPtr);
release_state_run_cleanup:
	enif_release_resource(statePtr);
	return NULL;
}

static ERL_NIF_TERM release_state_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	struct release_thread_state *threadStatePtr = enif_alloc_resource(
		releaseThreadStateType,
		sizeof(struct release_thread_state)
	);
	threadStatePtr->optsPtr = enif_thread_opts_create("state");
	threadStatePtr->envPtr = enif_alloc_env();

	if (argc != 3) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_local_pid(envPtr, argv[0], &(threadStatePtr->parentPID))) {
		return enif_make_badarg(envPtr);
	}
	threadStatePtr->ref = enif_make_copy(threadStatePtr->envPtr, argv[1]);
	threadStatePtr->vmState = enif_make_copy(threadStatePtr->envPtr, argv[2]);

	int status = enif_thread_create(
		"release_thread",
		&(threadStatePtr->tid),
		&release_state_run,
		threadStatePtr,
		threadStatePtr->optsPtr
	);
	if (status != 0) {
		return error(envPtr, "enif_thread_create for release_state failed");
	}
	ERL_NIF_TERM threadStateTerm = enif_make_resource(envPtr, threadStatePtr);
	enif_release_resource(threadStatePtr);
	return ok_tuple(envPtr, threadStateTerm);
}

static void init_thread_state_dtor(ErlNifEnv* env, void* obj)
{
	struct init_thread_state* threadStatePtr = (struct init_thread_state*) obj;

	enif_thread_join(threadStatePtr->tid, NULL);
	enif_thread_opts_destroy(threadStatePtr->optsPtr);
	enif_free_env(threadStatePtr->envPtr);
}

static void hash_thread_state_dtor(ErlNifEnv* env, void* obj)
{
	struct hash_thread_state* threadStatePtr = (struct hash_thread_state*) obj;

	enif_thread_join(threadStatePtr->tid, NULL);
	enif_thread_opts_destroy(threadStatePtr->optsPtr);
	enif_free_env(threadStatePtr->envPtr);
}

static void release_thread_state_dtor(ErlNifEnv* env, void* obj)
{
	struct release_thread_state* threadStatePtr = (struct release_thread_state*) obj;

	enif_thread_join(threadStatePtr->tid, NULL);
	enif_thread_opts_destroy(threadStatePtr->optsPtr);
	enif_free_env(threadStatePtr->envPtr);
}

static int load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	int flags = ERL_NIF_RT_CREATE;
	stateType = enif_open_resource_type(envPtr, NULL, "state", state_dtor, flags, NULL);
	initThreadStateType = enif_open_resource_type(
		envPtr,
		NULL,
		"init_thread_state",
		init_thread_state_dtor,
		flags,
		NULL
	);
	hashThreadStateType = enif_open_resource_type(
		envPtr,
		NULL,
		"hash_thread_state",
		hash_thread_state_dtor,
		flags,
		NULL
	);
	releaseThreadStateType = enif_open_resource_type(
		envPtr,
		NULL,
		"release_thread_state",
		release_thread_state_dtor,
		flags,
		NULL
	);

	if (stateType == NULL || initThreadStateType == NULL || hashThreadStateType == NULL || releaseThreadStateType == NULL) {
		return 1;
	} else {
		return 0;
	}
}

static ErlNifFunc nif_funcs[] = {
	{"init_fast", 4, init_fast_nif},
	{"init_light", 3, init_light_nif},
	{"hash_fast", 4, hash_fast_nif},
	{"hash_light", 4, hash_light_nif},
	{"release_state", 3, release_state_nif}
};

ERL_NIF_INIT(ar_mine_randomx, nif_funcs, load, NULL, NULL, NULL)
