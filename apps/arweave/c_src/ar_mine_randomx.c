#include <erl_nif.h>
#include <string.h>
#include "randomx.h"
#include "ar_mine_randomx.h"
#include <gmp.h>
#include <openssl/sha.h>
#include "sha-256.h"
#include "randomx_long_with_entropy.h"
#include "feistel_msgsize_key_cipher.h"
#include "vdf.h"

ErlNifResourceType* stateType;
ErlNifResourceType* vdfRandomxVmType;
// just for split sources
#include "ar_mine_vdf.h"

static ErlNifFunc nif_funcs[] = {
	{"init_fast_nif", 4, init_fast_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"init_light_nif", 3, init_light_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_fast_nif", 5, hash_fast_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_light_nif", 5, hash_light_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"bulk_hash_fast_nif", 13, bulk_hash_fast_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_fast_verify_nif", 6, hash_fast_verify_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_encrypt_chunk_nif", 7, randomx_encrypt_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_decrypt_chunk_nif", 8, randomx_decrypt_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_reencrypt_chunk_nif", 10, randomx_reencrypt_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_encrypt_composite_chunk_nif", 9, randomx_encrypt_composite_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_decrypt_composite_chunk_nif", 10, randomx_decrypt_composite_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_decrypt_composite_sub_chunk_nif", 10, randomx_decrypt_composite_sub_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_reencrypt_legacy_to_composite_chunk_nif", 11,
		randomx_reencrypt_legacy_to_composite_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"randomx_reencrypt_composite_to_composite_chunk_nif", 13,
		randomx_reencrypt_composite_to_composite_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_fast_long_with_entropy_nif", 6, hash_fast_long_with_entropy_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"hash_light_long_with_entropy_nif", 6, hash_light_long_with_entropy_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"bulk_hash_fast_long_with_entropy_nif", 14, bulk_hash_fast_long_with_entropy_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"release_state_nif", 1, release_state_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_sha2_nif", 5, vdf_sha2_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_parallel_sha_verify_nif", 8, vdf_parallel_sha_verify_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_parallel_sha_verify_with_reset_nif", 10, vdf_parallel_sha_verify_with_reset_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_mine_randomx, nif_funcs, load, NULL, NULL, NULL);

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

	release_randomx(statePtr);
	if (statePtr->lockPtr != NULL) {
		enif_rwlock_destroy(statePtr->lockPtr);
		statePtr->lockPtr = NULL;
	}
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

static ERL_NIF_TERM init(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[],
	hashing_mode mode
) {
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
	return error(envPtr, reason);
}

static ERL_NIF_TERM hash_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return randomx_hash_nif(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM hash_light_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return randomx_hash_nif(envPtr, argc, argv, HASHING_MODE_LIGHT);
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

static ERL_NIF_TERM decrypt_chunk(ErlNifEnv* envPtr,
		randomx_vm *machine, const unsigned char *input, const size_t inputSize,
		const unsigned char *inChunk, const size_t inChunkSize,
		unsigned char* outChunk, const size_t outChunkSize,
		const int randomxProgramCount) {

	randomx_decrypt_chunk(
		machine, input, inputSize, inChunk, inChunkSize, outChunk, randomxProgramCount);
	return make_output_binary(envPtr, outChunk, outChunkSize);
}

static ERL_NIF_TERM encrypt_chunk(ErlNifEnv* envPtr,
		randomx_vm *machine, const unsigned char *input, const size_t inputSize,
		const unsigned char *inChunk, const size_t inChunkSize,
		const int randomxProgramCount) {
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

static ERL_NIF_TERM encrypt_composite_chunk(ErlNifEnv* envPtr,
		randomx_vm *vmPtr, ErlNifBinary *inputDataPtr, ErlNifBinary *inputChunkPtr,
		const int subChunkCount, const int iterations,
		const int randomxRoundCount, const int jitEnabled,
		const int largePagesEnabled, const int hardwareAESEnabled) {

	unsigned char *paddedChunk = (unsigned char*)malloc(MAX_CHUNK_SIZE);
	if (inputChunkPtr->size == MAX_CHUNK_SIZE) {
		memcpy(paddedChunk, inputChunkPtr->data, inputChunkPtr->size);
	} else {
		memset(paddedChunk, 0, MAX_CHUNK_SIZE);
		memcpy(paddedChunk, inputChunkPtr->data, inputChunkPtr->size);
	}

	ERL_NIF_TERM encryptedChunkTerm;
	unsigned char* encryptedChunk = enif_make_new_binary(envPtr, MAX_CHUNK_SIZE,
			&encryptedChunkTerm);
	// Both MAX_CHUNK_SIZE and subChunkCount are multiples of 64 so all sub-chunks
	// are of the same size.
	uint32_t subChunkSize = MAX_CHUNK_SIZE / subChunkCount;
	uint32_t offset = 0;
	unsigned char key[PACKING_KEY_SIZE];
	for (int i = 0; i < subChunkCount; i++) {
		unsigned char* subChunk = paddedChunk + offset;
		unsigned char* encryptedSubChunk = (unsigned char*)malloc(subChunkSize);

		// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
		int offsetByteSize = 3;
		unsigned char offsetBytes[offsetByteSize];
		for (int k = 0; k < offsetByteSize; k++) {
			offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
		}
		SHA256_CTX sha256;
		SHA256_Init(&sha256);
		SHA256_Update(&sha256, inputDataPtr->data, inputDataPtr->size);
		SHA256_Update(&sha256, offsetBytes, offsetByteSize);
		SHA256_Final(key, &sha256);

		for (int j = 0; j < iterations; j++) {
			randomx_encrypt_chunk(
				vmPtr, key, PACKING_KEY_SIZE, subChunk, subChunkSize,
				encryptedSubChunk, randomxRoundCount);
			if (j < iterations - 1) {
				memcpy(subChunk, encryptedSubChunk, subChunkSize);
			}
		}
		memcpy(encryptedChunk + offset, encryptedSubChunk, subChunkSize);
		free(encryptedSubChunk);
		offset += subChunkSize;
	}
	free(paddedChunk);
	return encryptedChunkTerm;
}

static ERL_NIF_TERM decrypt_composite_chunk(ErlNifEnv* envPtr,
		randomx_vm *vmPtr, ErlNifBinary *inputDataPtr, ErlNifBinary *inputChunkPtr,
		const int outChunkLen, const int subChunkCount, const int iterations,
		const int randomxRoundCount, const int jitEnabled,
		const int largePagesEnabled, const int hardwareAESEnabled) {

	ERL_NIF_TERM decryptedChunkTerm;
	unsigned char* decryptedChunk = enif_make_new_binary(envPtr, outChunkLen,
			&decryptedChunkTerm);
	unsigned char* decryptedSubChunk;
	// Both MAX_CHUNK_SIZE and subChunkCount are multiples of 64 so all sub-chunks
	// are of the same size.
	uint32_t subChunkSize = outChunkLen / subChunkCount;
	uint32_t offset = 0;
	unsigned char key[PACKING_KEY_SIZE];
	for (int i = 0; i < subChunkCount; i++) {
		unsigned char* subChunk = inputChunkPtr->data + offset;
		decryptedSubChunk = (unsigned char*)malloc(subChunkSize);

		// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
		int offsetByteSize = 3;
		unsigned char offsetBytes[offsetByteSize];
		for (int k = 0; k < offsetByteSize; k++) {
			offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
		}
		SHA256_CTX sha256;
		SHA256_Init(&sha256);
		SHA256_Update(&sha256, inputDataPtr->data, inputDataPtr->size);
		SHA256_Update(&sha256, offsetBytes, offsetByteSize);
		SHA256_Final(key, &sha256);

		for (int j = 0; j < iterations; j++) {
			randomx_decrypt_chunk(
				vmPtr, key, PACKING_KEY_SIZE, subChunk, subChunkSize,
				decryptedSubChunk, randomxRoundCount);
			if (j < iterations - 1) {
				memcpy(subChunk, decryptedSubChunk, subChunkSize);
			}
		}
		memcpy(decryptedChunk + offset, decryptedSubChunk, subChunkSize);
		free(decryptedSubChunk);
		offset += subChunkSize;
	}
	return decryptedChunkTerm;
}

static ERL_NIF_TERM randomx_hash_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[],
	hashing_mode hashingMode
) {
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
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

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (hashingMode == HASHING_MODE_FAST), jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	randomx_calculate_hash(vmPtr, inputData.data, inputData.size, hashPtr);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE));
}

static ERL_NIF_TERM bulk_hash_fast_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	randomx_vm *vmPtr = NULL;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	randomx_flags flags;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary firstNonceBinary, secondNonceBinary, inputData, prevH, searchSpaceUpperBound;
	unsigned char nonce[RANDOMX_HASH_SIZE];
	unsigned char prevNonce[RANDOMX_HASH_SIZE];
	unsigned char segment[RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE];
	int hashingIterations;
	unsigned int pidCount, proxyPIDCount;
	ErlNifPid *pids, *proxyPIDs;

	mpz_t mpzH, mpzSearchSpaceUpperBound;
	mpz_t mpzSubspaceNumber, mpzSubspaces, mpzEvenSubspaceSize;
	mpz_t mpzSearchSpaceSize, mpzSearchSubspaceSize;
	mpz_t mpzSearchSpaceShare;
	mpz_t mpzSubspaceStart, mpzSubspaceSize, mpzSearchSubspaceStart;
	mpz_t mpzSeed, mpzSearchSubspaceByteSeed, mpzSearchSubspaceByte;
	mpz_t diff, sum1, rem, result;
	unsigned char seedBin[32], searchSubspaceByteSeedBin[32];
	bigInt encodedSubspaceNumber;
	size_t encodedSubspaceNumberLen;
	unsigned char bin[80];
	bigInt byte;
	size_t size;

	if (argc != 13) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &firstNonceBinary)) {
		return enif_make_badarg(envPtr);
	}
	if (firstNonceBinary.size != RANDOMX_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &secondNonceBinary)) {
		return enif_make_badarg(envPtr);
	}
	if (secondNonceBinary.size != RANDOMX_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[3], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (inputData.size != ARWEAVE_INPUT_DATA_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[4], &prevH)) {
		return enif_make_badarg(envPtr);
	}
	if (prevH.size != ARWEAVE_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[5], &searchSpaceUpperBound)) {
		return enif_make_badarg(envPtr);
	}
	if (searchSpaceUpperBound.size > BIG_NUM_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_list_length(envPtr, argv[6], &pidCount)) {
		return enif_make_badarg(envPtr);
	}
	pids = (ErlNifPid*) enif_alloc(pidCount * sizeof(ErlNifPid));
	ERL_NIF_TERM list = argv[6];
	ERL_NIF_TERM head;
	for (int i = 0; i < pidCount; i++) {
		if (!enif_get_list_cell(envPtr, list, &head, &list)) {
			enif_free(pids);
			return enif_make_badarg(envPtr);
		}
		if (!enif_get_local_pid(envPtr, head, &pids[i])) {
			enif_free(pids);
			return enif_make_badarg(envPtr);
		}
	}
	if (!enif_get_list_length(envPtr, argv[7], &proxyPIDCount)) {
		return enif_make_badarg(envPtr);
	}
	proxyPIDs = (ErlNifPid*) enif_alloc(proxyPIDCount * sizeof(ErlNifPid));
	list = argv[7];
	for (int i = 0; i < proxyPIDCount; i++) {
		if (!enif_get_list_cell(envPtr, list, &head, &list)) {
			enif_free(pids);
			enif_free(proxyPIDs);
			return enif_make_badarg(envPtr);
		}
		if (!enif_get_local_pid(envPtr, head, &proxyPIDs[i])) {
			enif_free(pids);
			enif_free(proxyPIDs);
			return enif_make_badarg(envPtr);
		}
	}
	// argv[8] is a reference, it is simply passed on.
	if (!enif_get_int(envPtr, argv[9], &hashingIterations)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[10], &jitEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[11], &largePagesEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[12], &hardwareAESEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}

	enif_rwlock_rlock(statePtr->lockPtr);
	if (statePtr->isRandomxReleased != 0) {
		enif_rwlock_runlock(statePtr->lockPtr);
		enif_free(pids);
		enif_free(proxyPIDs);
		return error(envPtr, "state has been released");
	}

	flags = RANDOMX_FLAG_FULL_MEM;
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
	vmPtr = randomx_create_vm(flags, statePtr->cachePtr, statePtr->datasetPtr);
	if (vmPtr == NULL) {
		enif_rwlock_runlock(statePtr->lockPtr);
		enif_free(pids);
		enif_free(proxyPIDs);
		return error(envPtr, "randomx_create_vm failed");
	}

	mpz_init_set_ui(mpzSubspaces, (unsigned long int) SPORA_SUBSPACES_COUNT);
	mpz_init(mpzEvenSubspaceSize);
	mpz_init(mpzSearchSpaceUpperBound);
	mpz_init(mpzH);
	mpz_init(mpzSubspaceNumber);
	mpz_init(mpzSearchSpaceSize);
	mpz_init(mpzSearchSpaceShare);
	mpz_init_set_ui(mpzSearchSpaceShare, (unsigned long int) SPORA_SEARCH_SPACE_SHARE);
	mpz_init(mpzSearchSubspaceSize);
	mpz_init(mpzSubspaceStart);
	mpz_init(diff);
	mpz_init(mpzSubspaceSize);
	mpz_init(mpzSeed);
	mpz_init(mpzSearchSubspaceStart);
	mpz_init(mpzSearchSubspaceByteSeed);
	mpz_init(mpzSearchSubspaceByte);
	mpz_init(sum1);
	mpz_init(rem);
	mpz_init(result);

	mpz_import(
		mpzSearchSpaceUpperBound,
		searchSpaceUpperBound.size,
		1,
		1,
		1,
		0,
		searchSpaceUpperBound.data
	);
	mpz_fdiv_q(mpzEvenSubspaceSize, mpzSearchSpaceUpperBound, mpzSubspaces);
	mpz_fdiv_q(mpzSearchSpaceSize, mpzSearchSpaceUpperBound, mpzSearchSpaceShare);
	mpz_fdiv_q(mpzSearchSubspaceSize, mpzSearchSpaceSize, mpzSubspaces);

	memcpy(nonce, firstNonceBinary.data, RANDOMX_HASH_SIZE);
	memcpy(segment, nonce, RANDOMX_HASH_SIZE);
	memcpy(segment + RANDOMX_HASH_SIZE, inputData.data, ARWEAVE_INPUT_DATA_SIZE);

	int pidCursor = 0, proxyPIDCursor = 0;
	randomx_calculate_hash_first(vmPtr, segment, RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE);
	for (int i = 0; i < hashingIterations; i++) {
		memcpy(prevNonce, nonce, RANDOMX_HASH_SIZE);
		if (i == 0) {
			memcpy(nonce, secondNonceBinary.data, RANDOMX_HASH_SIZE);
		} else {
			memcpy(nonce, hashPtr, RANDOMX_HASH_SIZE);
		}
		if (i == hashingIterations - 1) {
			randomx_calculate_hash_last(vmPtr, hashPtr);
		} else {
			memcpy(segment, nonce, RANDOMX_HASH_SIZE);
			memcpy(segment + RANDOMX_HASH_SIZE, inputData.data, ARWEAVE_INPUT_DATA_SIZE);
			randomx_calculate_hash_next(
				vmPtr,
				segment,
				RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE,
				hashPtr
			);
		}

		mpz_import(mpzH, BIG_NUM_SIZE, 1, 1, 1, 0, hashPtr);
		mpz_fdiv_r(mpzSubspaceNumber, mpzH, mpzSubspaces);
		mpz_mul(mpzSubspaceStart, mpzSubspaceNumber, mpzEvenSubspaceSize);
		mpz_sub(diff, mpzSearchSpaceUpperBound, mpzSubspaceStart);
		if (mpz_cmp(diff, mpzEvenSubspaceSize) <= 0) {
			mpz_set(mpzSubspaceSize, diff);
		} else {
			mpz_set(mpzSubspaceSize, mpzEvenSubspaceSize);
		}
		mpz_export(
			encodedSubspaceNumber, &encodedSubspaceNumberLen, 1, 1, 1, 0, mpzSubspaceNumber);
		memcpy(bin, prevH.data, prevH.size);
		if (encodedSubspaceNumberLen == 0) {
			bin[ARWEAVE_HASH_SIZE] = 0;
			encodedSubspaceNumberLen = 1;
		} else {
			memcpy(bin + ARWEAVE_HASH_SIZE, encodedSubspaceNumber, encodedSubspaceNumberLen);
		}
		calc_sha_256(seedBin, bin, ARWEAVE_HASH_SIZE + encodedSubspaceNumberLen);
		mpz_import(mpzSeed, 32, 1, 1, 1, 0, seedBin);
		mpz_fdiv_r(mpzSearchSubspaceStart, mpzSeed, mpzSubspaceSize);
		calc_sha_256(searchSubspaceByteSeedBin, hashPtr, BIG_NUM_SIZE);
		mpz_import(mpzSearchSubspaceByteSeed, 32, 1, 1, 1, 0, searchSubspaceByteSeedBin);
		mpz_fdiv_r(mpzSearchSubspaceByte, mpzSearchSubspaceByteSeed, mpzSearchSubspaceSize);
		mpz_add(sum1, mpzSearchSubspaceStart, mpzSearchSubspaceByte);
		mpz_fdiv_r(rem, sum1, mpzSubspaceSize);
		mpz_add(result, mpzSubspaceStart, rem);
		mpz_export(byte, &size, 1, 1, 1, 0, result);

		ERL_NIF_TERM byteTerm = make_output_binary(envPtr, byte, size);
		ERL_NIF_TERM hashTerm = make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE);
		ERL_NIF_TERM nonceTerm = make_output_binary(envPtr, prevNonce, RANDOMX_HASH_SIZE);
		ERL_NIF_TERM pidTerm = enif_make_pid(envPtr, &proxyPIDs[proxyPIDCursor]);
		ERL_NIF_TERM tupleTerm = enif_make_tuple5(
			envPtr,
			byteTerm,
			hashTerm,
			nonceTerm,
			pidTerm,
			argv[8]
		);
		enif_send(envPtr, &pids[pidCursor], NULL, tupleTerm);
		pidCursor++;
		if (pidCursor == pidCount) {
			pidCursor = 0;
		}
		proxyPIDCursor++;
		if (proxyPIDCursor == proxyPIDCount) {
			proxyPIDCursor = 0;
		}
	}
	randomx_destroy_vm(vmPtr);
	enif_rwlock_runlock(statePtr->lockPtr);
	enif_free(pids);
	enif_free(proxyPIDs);
	mpz_clear(mpzH);
	mpz_clear(diff);
	mpz_clear(sum1);
	mpz_clear(rem);
	mpz_clear(mpzSearchSpaceUpperBound);
	mpz_clear(mpzSubspaceNumber);
	mpz_clear(mpzSubspaces);
	mpz_clear(mpzEvenSubspaceSize);
	mpz_clear(mpzSearchSpaceSize);
	mpz_clear(mpzSearchSubspaceSize);
	mpz_clear(mpzSearchSpaceShare);
	mpz_clear(mpzSubspaceStart);
	mpz_clear(mpzSubspaceSize);
	mpz_clear(mpzSearchSubspaceStart);
	mpz_clear(mpzSeed);
	mpz_clear(mpzSearchSubspaceByteSeed);
	mpz_clear(mpzSearchSubspaceByte);
	mpz_clear(result);
	return enif_make_atom(envPtr, "ok");
}

static ERL_NIF_TERM hash_fast_verify_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary difficulty, inputData;

	if (argc != 6) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[1], &difficulty)) {
		return enif_make_badarg(envPtr);
	}
	if (difficulty.size != RANDOMX_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_iolist_as_binary(envPtr, argv[2], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, 1, jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	randomx_calculate_hash(vmPtr, inputData.data, inputData.size, hashPtr);
	if (validate_hash(hashPtr, difficulty.data) > 0) {
		ERL_NIF_TERM hashTerm = make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE);
		randomx_destroy_vm(vmPtr);
		enif_rwlock_runlock(statePtr->lockPtr);
		return solution_tuple(envPtr, hashTerm);
	}

	destroy_vm(statePtr, vmPtr);

	return enif_make_atom(envPtr, "false");
}


static ERL_NIF_TERM randomx_encrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 7) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
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
	randomx_vm *vmPtr = create_vm(statePtr, 1, jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	ERL_NIF_TERM outChunkTerm = encrypt_chunk(envPtr, vmPtr,
		inputData.data, inputData.size, inputChunk.data, inputChunk.size, randomxRoundCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, outChunkTerm);
}

static ERL_NIF_TERM randomx_decrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int outChunkLen, randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 8) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
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
	randomx_vm *vmPtr = create_vm(statePtr, 1, jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
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

static ERL_NIF_TERM randomx_reencrypt_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int chunkSize, decryptRandomxRoundCount, encryptRandomxRoundCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary decryptKey;
	ErlNifBinary encryptKey;
	ErlNifBinary inputChunk;

	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
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
	randomx_vm *vmPtr = create_vm(statePtr, 1, jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
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

static ERL_NIF_TERM randomx_encrypt_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	// RandomX rounds per sub-chunk.
	int randomxRoundCount;
	// RandomX iterations (randomxRoundCount each) per sub-chunk.
	int iterations;
	// The number of sub-chunks in the chunk.
	int subChunkCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 9) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk) ||
		inputChunk.size == 0 ||
		inputChunk.size > MAX_CHUNK_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[6], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[7], &iterations) ||
		iterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &subChunkCount) ||
		subChunkCount < 1 ||
		MAX_CHUNK_SIZE % subChunkCount != 0 ||
		(MAX_CHUNK_SIZE / subChunkCount) % 64 != 0 ||
		subChunkCount > (MAX_CHUNK_SIZE / 64)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, 1,
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	ERL_NIF_TERM encryptedChunkTerm = encrypt_composite_chunk(envPtr, vmPtr, &inputData,
			&inputChunk, subChunkCount, iterations, randomxRoundCount,
			jitEnabled, largePagesEnabled, hardwareAESEnabled);
	destroy_vm(statePtr, vmPtr);
	return ok_tuple(envPtr, encryptedChunkTerm);
}

static ERL_NIF_TERM randomx_decrypt_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int outChunkLen;
	// RandomX rounds per sub-chunk.
	int randomxRoundCount;
	// RandomX iterations (randomxRoundCount each) per sub-chunk.
	int iterations;
	// The number of sub-chunks in the chunk.
	int subChunkCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &outChunkLen) ||
		outChunkLen > MAX_CHUNK_SIZE ||
		outChunkLen < 64 ||
		inputChunk.size != outChunkLen) {
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
	if (!enif_get_int(envPtr, argv[7], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &iterations) ||
		iterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[9], &subChunkCount) ||
		subChunkCount < 1 ||
		outChunkLen % subChunkCount != 0 ||
		(outChunkLen / subChunkCount) % 64 != 0 ||
		subChunkCount > (outChunkLen / 64)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, 1,
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}
	ERL_NIF_TERM decryptedChunkTerm = decrypt_composite_chunk(envPtr, vmPtr,
			&inputData, &inputChunk, outChunkLen, subChunkCount, iterations,
			randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, decryptedChunkTerm);
}

static ERL_NIF_TERM randomx_decrypt_composite_sub_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int outChunkLen;
	// RandomX rounds per sub-chunk.
	int randomxRoundCount;
	// RandomX iterations (randomxRoundCount each) per sub-chunk.
	int iterations;
	// The relative sub-chunk start offset. We add the chunk size to it, encode the result,
	// add it to the base packing key, and SHA256-hash it to get the packing key.
	uint32_t offset;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &outChunkLen) ||
		outChunkLen > MAX_CHUNK_SIZE ||
		outChunkLen < 64 ||
		inputChunk.size != outChunkLen ) {
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
	if (!enif_get_int(envPtr, argv[7], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &iterations) ||
		iterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[9], &offset) ||
		offset < 0 ||
		offset > MAX_CHUNK_SIZE) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	ERL_NIF_TERM decryptedSubChunkTerm;
	unsigned char* decryptedSubChunk = enif_make_new_binary(envPtr, outChunkLen,
			&decryptedSubChunkTerm);
	uint32_t subChunkSize = outChunkLen;
	unsigned char key[PACKING_KEY_SIZE];

	randomx_vm *vmPtr = create_vm(statePtr, 1,
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	unsigned char* subChunk = (unsigned char*)malloc(inputChunk.size);
	memcpy(subChunk, inputChunk.data, inputChunk.size);

	// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
	int offsetByteSize = 3;
	unsigned char offsetBytes[offsetByteSize];
	for (int k = 0; k < offsetByteSize; k++) {
		offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
	}
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, inputData.data, inputData.size);
	SHA256_Update(&sha256, offsetBytes, offsetByteSize);
	SHA256_Final(key, &sha256);

	for (int j = 0; j < iterations; j++) {
		randomx_decrypt_chunk(vmPtr, key, PACKING_KEY_SIZE, subChunk, subChunkSize,
			decryptedSubChunk, randomxRoundCount);
		if (j < iterations - 1) {
			memcpy(subChunk, decryptedSubChunk, subChunkSize);
		}
	}
	free(subChunk);
	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, decryptedSubChunkTerm);
}

static ERL_NIF_TERM randomx_reencrypt_legacy_to_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int decryptRandomxRoundCount, encryptRandomxRoundCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	int subChunkCount, iterations;
	struct state* statePtr;
	ErlNifBinary decryptKey;
	ErlNifBinary encryptKey;
	ErlNifBinary inputChunk;

	if (argc != 11) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &decryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &encryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[3], &inputChunk) ||
			inputChunk.size != MAX_CHUNK_SIZE) {
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
	if (!enif_get_int(envPtr, argv[7], &decryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &encryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[9], &iterations) ||
		iterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[10], &subChunkCount) ||
		subChunkCount < 1 ||
		MAX_CHUNK_SIZE % subChunkCount != 0 ||
		(MAX_CHUNK_SIZE / subChunkCount) % 64 != 0 ||
		subChunkCount > (MAX_CHUNK_SIZE / 64)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, 1,
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	unsigned char decryptedChunk[MAX_CHUNK_SIZE];
	ERL_NIF_TERM decryptedChunkTerm = decrypt_chunk(envPtr, vmPtr,
		decryptKey.data, decryptKey.size, inputChunk.data, inputChunk.size,
		decryptedChunk, inputChunk.size, decryptRandomxRoundCount);
	ErlNifBinary decryptedChunkBin;
    if (!enif_inspect_binary(envPtr, decryptedChunkTerm, &decryptedChunkBin)) {
		destroy_vm(statePtr, vmPtr);
        return enif_make_badarg(envPtr);
    }
	ERL_NIF_TERM reencryptedChunkTerm = encrypt_composite_chunk(envPtr, vmPtr, &encryptKey,
			&decryptedChunkBin, subChunkCount, iterations, encryptRandomxRoundCount,
			jitEnabled, largePagesEnabled, hardwareAESEnabled);
	destroy_vm(statePtr, vmPtr);
	return ok_tuple2(envPtr, reencryptedChunkTerm, decryptedChunkTerm);
}

static ERL_NIF_TERM randomx_reencrypt_composite_to_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	int decryptRandomxRoundCount, encryptRandomxRoundCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	int decryptSubChunkCount, encryptSubChunkCount, decryptIterations, encryptIterations;
	struct state* statePtr;
	ErlNifBinary decryptKey;
	ErlNifBinary encryptKey;
	ErlNifBinary inputChunk;
	ERL_NIF_TERM inputChunkTerm;

	if (argc != 13) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &decryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &encryptKey)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[3], &inputChunk) ||
			inputChunk.size != MAX_CHUNK_SIZE) {
		return enif_make_badarg(envPtr);
	}
	inputChunkTerm = argv[3];
	if (!enif_get_int(envPtr, argv[4], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[6], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[7], &decryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[8], &encryptRandomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[9], &decryptIterations) ||
		decryptIterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[10], &encryptIterations) ||
		encryptIterations < 1) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[11], &decryptSubChunkCount) ||
		decryptSubChunkCount < 1 ||
		MAX_CHUNK_SIZE % decryptSubChunkCount != 0 ||
		(MAX_CHUNK_SIZE / decryptSubChunkCount) % 64 != 0 ||
		decryptSubChunkCount > (MAX_CHUNK_SIZE / 64)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[12], &encryptSubChunkCount) ||
		encryptSubChunkCount < 1 ||
		MAX_CHUNK_SIZE % encryptSubChunkCount != 0 ||
		(MAX_CHUNK_SIZE / encryptSubChunkCount) % 64 != 0 ||
		encryptSubChunkCount > (MAX_CHUNK_SIZE / 64)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, 1,
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	int keysMatch = 0;
	if (decryptKey.size == encryptKey.size) {
		if (memcmp(decryptKey.data, encryptKey.data, decryptKey.size) == 0) {
			keysMatch = 1;
		}
	}
	int encryptionsMatch = 0;
	if (keysMatch && (decryptSubChunkCount == encryptSubChunkCount) &&
			(decryptRandomxRoundCount == encryptRandomxRoundCount)) {
		encryptionsMatch = 1;
	}

	if (encryptionsMatch && (encryptIterations <= decryptIterations)) {
		destroy_vm(statePtr, vmPtr);
		return enif_make_badarg(envPtr);
	}

	unsigned char decryptedChunk[MAX_CHUNK_SIZE];
	ErlNifBinary *decryptedChunkBinPtr;
	ERL_NIF_TERM decryptedChunkTerm;
	if (!encryptionsMatch) {
		decryptedChunkTerm = decrypt_composite_chunk(envPtr, vmPtr,
				&decryptKey, &inputChunk, inputChunk.size, decryptSubChunkCount,
				decryptIterations, decryptRandomxRoundCount, jitEnabled,
				largePagesEnabled, hardwareAESEnabled);
		ErlNifBinary decryptedChunkBin;
		if (!enif_inspect_binary(envPtr, decryptedChunkTerm, &decryptedChunkBin)) {
			destroy_vm(statePtr, vmPtr);
			return enif_make_badarg(envPtr);
		}
		decryptedChunkBinPtr = &decryptedChunkBin;
	} else {
		decryptedChunkBinPtr = &inputChunk;
		decryptedChunkTerm = inputChunkTerm;
	}
	int iterations = encryptIterations;
	if (encryptionsMatch) {
		iterations = encryptIterations - decryptIterations;
	}

	ERL_NIF_TERM reencryptedChunkTerm = encrypt_composite_chunk(envPtr, vmPtr, &encryptKey,
			decryptedChunkBinPtr, encryptSubChunkCount, iterations, encryptRandomxRoundCount,
			jitEnabled, largePagesEnabled, hardwareAESEnabled);
	destroy_vm(statePtr, vmPtr);
	return ok_tuple2(envPtr, reencryptedChunkTerm, decryptedChunkTerm);
}

static ERL_NIF_TERM hash_fast_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return randomx_hash_long_with_entropy_nif(envPtr, argc, argv, HASHING_MODE_FAST);
}

static ERL_NIF_TERM hash_light_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return randomx_hash_long_with_entropy_nif(envPtr, argc, argv, HASHING_MODE_LIGHT);
}

static ERL_NIF_TERM randomx_hash_long_with_entropy_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[],
	hashing_mode hashingMode
) {
	int randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary inputData;

	if (argc != 6) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &randomxRoundCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[5], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int isRandomxReleased;
	randomx_vm *vmPtr = create_vm(statePtr, (hashingMode == HASHING_MODE_FAST), jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error(envPtr, "state has been released");
		}
		return error(envPtr, "randomx_create_vm failed");
	}

	ERL_NIF_TERM entropyPtrTerm;
	unsigned char* entropyPtr = enif_make_new_binary(envPtr, RANDOMX_ENTROPY_SIZE, &entropyPtrTerm);
	randomx_calculate_hash_long_with_entropy(vmPtr, inputData.data, inputData.size, hashPtr, entropyPtr, randomxRoundCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple2(envPtr, make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE), entropyPtrTerm);
}

static ERL_NIF_TERM bulk_hash_fast_long_with_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	randomx_vm *vmPtr = NULL;
	int randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	randomx_flags flags;
	unsigned char hashPtr[RANDOMX_HASH_SIZE];
	struct state* statePtr;
	ErlNifBinary firstNonceBinary, secondNonceBinary, inputData, prevH, searchSpaceUpperBound;
	unsigned char nonce[RANDOMX_HASH_SIZE];
	unsigned char prevNonce[RANDOMX_HASH_SIZE];
	unsigned char segment[RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE];
	int hashingIterations;
	unsigned int pidCount, proxyPIDCount;
	ErlNifPid *pids, *proxyPIDs;

	mpz_t mpzH, mpzSearchSpaceUpperBound;
	mpz_t mpzSubspaceNumber, mpzSubspaces, mpzEvenSubspaceSize;
	mpz_t mpzSearchSpaceSize, mpzSearchSubspaceSize;
	mpz_t mpzSearchSpaceShare;
	mpz_t mpzSubspaceStart, mpzSubspaceSize, mpzSearchSubspaceStart;
	mpz_t mpzSeed, mpzSearchSubspaceByteSeed, mpzSearchSubspaceByte;
	mpz_t diff, sum1, rem, result;
	unsigned char seedBin[32], searchSubspaceByteSeedBin[32];
	bigInt encodedSubspaceNumber;
	size_t encodedSubspaceNumberLen;
	unsigned char bin[80];
	bigInt byte;
	size_t size;

	if (argc != 14) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &firstNonceBinary)) {
		return enif_make_badarg(envPtr);
	}
	if (firstNonceBinary.size != RANDOMX_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &secondNonceBinary)) {
		return enif_make_badarg(envPtr);
	}
	if (secondNonceBinary.size != RANDOMX_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[3], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (inputData.size != ARWEAVE_INPUT_DATA_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[4], &prevH)) {
		return enif_make_badarg(envPtr);
	}
	if (prevH.size != ARWEAVE_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[5], &searchSpaceUpperBound)) {
		return enif_make_badarg(envPtr);
	}
	if (searchSpaceUpperBound.size > BIG_NUM_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_list_length(envPtr, argv[6], &pidCount)) {
		return enif_make_badarg(envPtr);
	}
	pids = (ErlNifPid*) enif_alloc(pidCount * sizeof(ErlNifPid));
	ERL_NIF_TERM list = argv[6];
	ERL_NIF_TERM head;
	for (int i = 0; i < pidCount; i++) {
		if (!enif_get_list_cell(envPtr, list, &head, &list)) {
			enif_free(pids);
			return enif_make_badarg(envPtr);
		}
		if (!enif_get_local_pid(envPtr, head, &pids[i])) {
			enif_free(pids);
			return enif_make_badarg(envPtr);
		}
	}
	if (!enif_get_list_length(envPtr, argv[7], &proxyPIDCount)) {
		return enif_make_badarg(envPtr);
	}
	proxyPIDs = (ErlNifPid*) enif_alloc(proxyPIDCount * sizeof(ErlNifPid));
	list = argv[7];
	for (int i = 0; i < proxyPIDCount; i++) {
		if (!enif_get_list_cell(envPtr, list, &head, &list)) {
			enif_free(pids);
			enif_free(proxyPIDs);
			return enif_make_badarg(envPtr);
		}
		if (!enif_get_local_pid(envPtr, head, &proxyPIDs[i])) {
			enif_free(pids);
			enif_free(proxyPIDs);
			return enif_make_badarg(envPtr);
		}
	}
	// argv[8] is a reference, it is simply passed on.
	if (!enif_get_int(envPtr, argv[9], &hashingIterations)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[10], &randomxRoundCount)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[11], &jitEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[12], &largePagesEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[13], &hardwareAESEnabled)) {
		enif_free(pids);
		enif_free(proxyPIDs);
		return enif_make_badarg(envPtr);
	}

	enif_rwlock_rlock(statePtr->lockPtr);
	if (statePtr->isRandomxReleased != 0) {
		enif_rwlock_runlock(statePtr->lockPtr);
		enif_free(pids);
		enif_free(proxyPIDs);
		return error(envPtr, "state has been released");
	}

	flags = RANDOMX_FLAG_FULL_MEM;
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
	vmPtr = randomx_create_vm(flags, statePtr->cachePtr, statePtr->datasetPtr);
	if (vmPtr == NULL) {
		enif_rwlock_runlock(statePtr->lockPtr);
		enif_free(pids);
		enif_free(proxyPIDs);
		return error(envPtr, "randomx_create_vm failed");
	}

	mpz_init_set_ui(mpzSubspaces, (unsigned long int) SPORA_SUBSPACES_COUNT);
	mpz_init(mpzEvenSubspaceSize);
	mpz_init(mpzSearchSpaceUpperBound);
	mpz_init(mpzH);
	mpz_init(mpzSubspaceNumber);
	mpz_init(mpzSearchSpaceSize);
	mpz_init(mpzSearchSpaceShare);
	mpz_init_set_ui(mpzSearchSpaceShare, (unsigned long int) SPORA_SEARCH_SPACE_SHARE);
	mpz_init(mpzSearchSubspaceSize);
	mpz_init(mpzSubspaceStart);
	mpz_init(diff);
	mpz_init(mpzSubspaceSize);
	mpz_init(mpzSeed);
	mpz_init(mpzSearchSubspaceStart);
	mpz_init(mpzSearchSubspaceByteSeed);
	mpz_init(mpzSearchSubspaceByte);
	mpz_init(sum1);
	mpz_init(rem);
	mpz_init(result);

	mpz_import(
		mpzSearchSpaceUpperBound,
		searchSpaceUpperBound.size,
		1,
		1,
		1,
		0,
		searchSpaceUpperBound.data
	);
	mpz_fdiv_q(mpzEvenSubspaceSize, mpzSearchSpaceUpperBound, mpzSubspaces);
	mpz_fdiv_q(mpzSearchSpaceSize, mpzSearchSpaceUpperBound, mpzSearchSpaceShare);
	mpz_fdiv_q(mpzSearchSubspaceSize, mpzSearchSpaceSize, mpzSubspaces);

	memcpy(nonce, firstNonceBinary.data, RANDOMX_HASH_SIZE);
	memcpy(segment, nonce, RANDOMX_HASH_SIZE);
	memcpy(segment + RANDOMX_HASH_SIZE, inputData.data, ARWEAVE_INPUT_DATA_SIZE);

	int pidCursor = 0, proxyPIDCursor = 0;
	randomx_calculate_hash_long_with_entropy_first(vmPtr, segment, RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE);
	for (int i = 0; i < hashingIterations; i++) {
		memcpy(prevNonce, nonce, RANDOMX_HASH_SIZE);
		if (i == 0) {
			memcpy(nonce, secondNonceBinary.data, RANDOMX_HASH_SIZE);
		} else {
			memcpy(nonce, hashPtr, RANDOMX_HASH_SIZE);
		}

		ERL_NIF_TERM entropyPtrTerm;
		unsigned char* entropyPtr = enif_make_new_binary(envPtr, RANDOMX_ENTROPY_SIZE, &entropyPtrTerm);
		if (i == hashingIterations - 1) {
			randomx_calculate_hash_long_with_entropy_last(vmPtr, hashPtr, entropyPtr, randomxRoundCount);
		} else {
			memcpy(segment, nonce, RANDOMX_HASH_SIZE);
			memcpy(segment + RANDOMX_HASH_SIZE, inputData.data, ARWEAVE_INPUT_DATA_SIZE);
			randomx_calculate_hash_long_with_entropy_next(
				vmPtr,
				segment,
				RANDOMX_HASH_SIZE + ARWEAVE_INPUT_DATA_SIZE,
				hashPtr,
				entropyPtr,
				randomxRoundCount
			);
		}

		mpz_import(mpzH, BIG_NUM_SIZE, 1, 1, 1, 0, hashPtr);
		mpz_fdiv_r(mpzSubspaceNumber, mpzH, mpzSubspaces);
		mpz_mul(mpzSubspaceStart, mpzSubspaceNumber, mpzEvenSubspaceSize);
		mpz_sub(diff, mpzSearchSpaceUpperBound, mpzSubspaceStart);
		if (mpz_cmp(diff, mpzEvenSubspaceSize) <= 0) {
			mpz_set(mpzSubspaceSize, diff);
		} else {
			mpz_set(mpzSubspaceSize, mpzEvenSubspaceSize);
		}
		mpz_export(
			encodedSubspaceNumber, &encodedSubspaceNumberLen, 1, 1, 1, 0, mpzSubspaceNumber);
		memcpy(bin, prevH.data, prevH.size);
		if (encodedSubspaceNumberLen == 0) {
			bin[ARWEAVE_HASH_SIZE] = 0;
			encodedSubspaceNumberLen = 1;
		} else {
			memcpy(bin + ARWEAVE_HASH_SIZE, encodedSubspaceNumber, encodedSubspaceNumberLen);
		}
		calc_sha_256(seedBin, bin, ARWEAVE_HASH_SIZE + encodedSubspaceNumberLen);
		mpz_import(mpzSeed, 32, 1, 1, 1, 0, seedBin);
		mpz_fdiv_r(mpzSearchSubspaceStart, mpzSeed, mpzSubspaceSize);
		calc_sha_256(searchSubspaceByteSeedBin, hashPtr, BIG_NUM_SIZE);
		mpz_import(mpzSearchSubspaceByteSeed, 32, 1, 1, 1, 0, searchSubspaceByteSeedBin);
		mpz_fdiv_r(mpzSearchSubspaceByte, mpzSearchSubspaceByteSeed, mpzSearchSubspaceSize);
		mpz_add(sum1, mpzSearchSubspaceStart, mpzSearchSubspaceByte);
		mpz_fdiv_r(rem, sum1, mpzSubspaceSize);
		mpz_add(result, mpzSubspaceStart, rem);
		mpz_export(byte, &size, 1, 1, 1, 0, result);

		ERL_NIF_TERM byteTerm = make_output_binary(envPtr, byte, size);
		ERL_NIF_TERM hashTerm = make_output_binary(envPtr, hashPtr, RANDOMX_HASH_SIZE);
		ERL_NIF_TERM nonceTerm = make_output_binary(envPtr, prevNonce, RANDOMX_HASH_SIZE);
		ERL_NIF_TERM pidTerm = enif_make_pid(envPtr, &proxyPIDs[proxyPIDCursor]);
		ERL_NIF_TERM tupleTerm = enif_make_tuple6(
			envPtr,
			byteTerm,
			hashTerm,
			entropyPtrTerm,
			nonceTerm,
			pidTerm,
			argv[8]
		);
		enif_send(envPtr, &pids[pidCursor], NULL, tupleTerm);
		pidCursor++;
		if (pidCursor == pidCount) {
			pidCursor = 0;
		}
		proxyPIDCursor++;
		if (proxyPIDCursor == proxyPIDCount) {
			proxyPIDCursor = 0;
		}
	}
	randomx_destroy_vm(vmPtr);
	enif_rwlock_runlock(statePtr->lockPtr);
	enif_free(pids);
	enif_free(proxyPIDs);
	mpz_clear(mpzH);
	mpz_clear(diff);
	mpz_clear(sum1);
	mpz_clear(rem);
	mpz_clear(mpzSearchSpaceUpperBound);
	mpz_clear(mpzSubspaceNumber);
	mpz_clear(mpzSubspaces);
	mpz_clear(mpzEvenSubspaceSize);
	mpz_clear(mpzSearchSpaceSize);
	mpz_clear(mpzSearchSubspaceSize);
	mpz_clear(mpzSearchSpaceShare);
	mpz_clear(mpzSubspaceStart);
	mpz_clear(mpzSubspaceSize);
	mpz_clear(mpzSearchSubspaceStart);
	mpz_clear(mpzSeed);
	mpz_clear(mpzSearchSubspaceByteSeed);
	mpz_clear(mpzSearchSubspaceByte);
	mpz_clear(result);
	return enif_make_atom(envPtr, "ok");
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

static ERL_NIF_TERM solution_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM hashTerm) {
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "true"), hashTerm);
}

static ERL_NIF_TERM ok_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "ok"), term);
}

static ERL_NIF_TERM ok_tuple2(ErlNifEnv* envPtr, ERL_NIF_TERM term1, ERL_NIF_TERM term2)
{
	return enif_make_tuple3(envPtr, enif_make_atom(envPtr, "ok"), term1, term2);
}

static ERL_NIF_TERM error_tuple(ErlNifEnv* envPtr, ERL_NIF_TERM term)
{
	return enif_make_tuple2(envPtr, enif_make_atom(envPtr, "error"), term);
}

static ERL_NIF_TERM error(ErlNifEnv* envPtr, const char* reason)
{
	return error_tuple(envPtr, enif_make_string(envPtr, reason, ERL_NIF_LATIN1));
}

static ERL_NIF_TERM make_output_binary(ErlNifEnv* envPtr, unsigned char *dataPtr, size_t size)
{
	ERL_NIF_TERM outputTerm;
	unsigned char *outputTermDataPtr;

	outputTermDataPtr = enif_make_new_binary(envPtr, size, &outputTerm);
	memcpy(outputTermDataPtr, dataPtr, size);
	return outputTerm;
}

static int validate_hash(
	unsigned char hash[RANDOMX_HASH_SIZE],
	unsigned char difficulty[RANDOMX_HASH_SIZE]
) {
	return memcmp(hash, difficulty, RANDOMX_HASH_SIZE);
}
