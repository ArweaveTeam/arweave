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
static ERL_NIF_TERM rx4096_encrypt_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
);
static ERL_NIF_TERM rx4096_decrypt_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
);
static ERL_NIF_TERM rx4096_decrypt_composite_sub_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
);
static ERL_NIF_TERM rx4096_reencrypt_composite_chunk_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
);

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
	// MAX_CHUNK_SIZE / subChunkCount is a multiple of 64 so all sub-chunks
	// are of the same size.
	uint32_t subChunkSize = MAX_CHUNK_SIZE / subChunkCount;
	uint32_t offset = 0;
	unsigned char key[PACKING_KEY_SIZE];
	// Encrypt each sub-chunk independently and then concatenate the encrypted sub-chunks
	// to yield encrypted composite chunk.
	for (int i = 0; i < subChunkCount; i++) {
		unsigned char* subChunk = paddedChunk + offset;
		unsigned char* encryptedSubChunk = (unsigned char*)malloc(subChunkSize);

		// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
		int offsetByteSize = 3;
		unsigned char offsetBytes[offsetByteSize];
		// Byte string representation of the sub-chunk start offset: i * subChunkSize.
		for (int k = 0; k < offsetByteSize; k++) {
			offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
		}
		// Sub-chunk encryption key is the SHA256 hash of the concatenated
		// input data and the sub-chunk start offset.
		SHA256_CTX sha256;
		SHA256_Init(&sha256);
		SHA256_Update(&sha256, inputDataPtr->data, inputDataPtr->size);
		SHA256_Update(&sha256, offsetBytes, offsetByteSize);
		SHA256_Final(key, &sha256);

		// Sequentially encrypt each sub-chunk 'iterations' times.
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

	unsigned char *chunk = (unsigned char*)malloc(MAX_CHUNK_SIZE);
	memcpy(chunk, inputChunkPtr->data, inputChunkPtr->size);

	ERL_NIF_TERM decryptedChunkTerm;
	unsigned char* decryptedChunk = enif_make_new_binary(envPtr, outChunkLen,
			&decryptedChunkTerm);
	unsigned char* decryptedSubChunk;
	// outChunkLen / subChunkCount is a multiple of 64 so all sub-chunks
	// are of the same size.
	uint32_t subChunkSize = outChunkLen / subChunkCount;
	uint32_t offset = 0;
	unsigned char key[PACKING_KEY_SIZE];
	// Decrypt each sub-chunk independently and then concatenate the decrypted sub-chunks
	// to yield encrypted composite chunk.
	for (int i = 0; i < subChunkCount; i++) {
		unsigned char* subChunk = chunk + offset;
		decryptedSubChunk = (unsigned char*)malloc(subChunkSize);

		// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
		int offsetByteSize = 3;
		unsigned char offsetBytes[offsetByteSize];
		// Byte string representation of the sub-chunk start offset: i * subChunkSize.
		for (int k = 0; k < offsetByteSize; k++) {
			offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
		}
		// Sub-chunk encryption key is the SHA256 hash of the concatenated
		// input data and the sub-chunk start offset.
		SHA256_CTX sha256;
		SHA256_Init(&sha256);
		SHA256_Update(&sha256, inputDataPtr->data, inputDataPtr->size);
		SHA256_Update(&sha256, offsetBytes, offsetByteSize);
		SHA256_Final(key, &sha256);

		// Sequentially decrypt each sub-chunk 'iterations' times.
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
	free(chunk);
	return decryptedChunkTerm;
}

static ERL_NIF_TERM rx4096_encrypt_composite_chunk_nif(
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
		return error_tuple(envPtr, "failed to read state");
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
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
	}

	ERL_NIF_TERM encryptedChunkTerm = encrypt_composite_chunk(envPtr, vmPtr, &inputData,
			&inputChunk, subChunkCount, iterations, randomxRoundCount,
			jitEnabled, largePagesEnabled, hardwareAESEnabled);
	destroy_vm(statePtr, vmPtr);
	return ok_tuple(envPtr, encryptedChunkTerm);
}

static ERL_NIF_TERM rx4096_decrypt_composite_chunk_nif(
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
		return error_tuple(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputData)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inputChunk) ||
		inputChunk.size == 0 ||
		inputChunk.size > MAX_CHUNK_SIZE) {
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
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
	}
	ERL_NIF_TERM decryptedChunkTerm = decrypt_composite_chunk(envPtr, vmPtr,
			&inputData, &inputChunk, outChunkLen, subChunkCount, iterations,
			randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple(envPtr, decryptedChunkTerm);
}

static ERL_NIF_TERM rx4096_decrypt_composite_sub_chunk_nif(
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
		return error_tuple(envPtr, "failed to read state");
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

	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
	}

	unsigned char* subChunk = (unsigned char*)malloc(inputChunk.size);
	memcpy(subChunk, inputChunk.data, inputChunk.size);

	// 3 bytes is sufficient to represent offsets up to at most MAX_CHUNK_SIZE.
	int offsetByteSize = 3;
	unsigned char offsetBytes[offsetByteSize];
	for (int k = 0; k < offsetByteSize; k++) {
		offsetBytes[k] = ((offset + subChunkSize) >> (8 * (offsetByteSize - 1 - k))) & 0xFF;
	}
	// Sub-chunk encryption key is the SHA256 hash of the concatenated
	// input data and the sub-chunk start offset.
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, inputData.data, inputData.size);
	SHA256_Update(&sha256, offsetBytes, offsetByteSize);
	SHA256_Final(key, &sha256);

	// Sequentially decrypt each sub-chunk 'iterations' times.
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

static ERL_NIF_TERM rx4096_reencrypt_composite_chunk_nif(
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
		return error_tuple(envPtr, "failed to read state");
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
	randomx_vm *vmPtr = create_vm(statePtr, (statePtr->mode == HASHING_MODE_FAST),
			jitEnabled, largePagesEnabled, hardwareAESEnabled, &isRandomxReleased);
	if (vmPtr == NULL) {
		if (isRandomxReleased != 0) {
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
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

static ErlNifFunc rx4096_funcs[] = {
	{"rx4096_info_nif", 1, rx4096_info_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_init_nif", 5, rx4096_init_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_hash_nif", 5, rx4096_hash_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_encrypt_composite_chunk_nif", 9, rx4096_encrypt_composite_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_decrypt_composite_chunk_nif", 10, rx4096_decrypt_composite_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_decrypt_composite_sub_chunk_nif", 10, rx4096_decrypt_composite_sub_chunk_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rx4096_reencrypt_composite_chunk_nif", 13,
		rx4096_reencrypt_composite_chunk_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_rx4096_nif, rx4096_funcs, rx4096_load, NULL, NULL, NULL);
