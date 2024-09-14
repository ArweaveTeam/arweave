#include <string.h>
#include <openssl/sha.h>
#include <ar_nif.h>
#include "../randomx_long_with_entropy.h"
#include "../feistel_msgsize_key_cipher.h"

#include "../ar_randomx_impl.h"

const int MAX_CHUNK_SIZE = 256*1024;

static int rx512_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info);
static ERL_NIF_TERM rx512_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_encrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_decrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rx512_reencrypt_chunk_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);

static int rx512_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	return load(envPtr, priv, info);
}

static ERL_NIF_TERM rx512_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return info_nif("rx512", envPtr, argc, argv);
}

static ERL_NIF_TERM rx512_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init_nif(envPtr, argc, argv);
}

static ERL_NIF_TERM rx512_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_nif(envPtr, argc, argv);
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

static ERL_NIF_TERM rx512_encrypt_chunk_nif(
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
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
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
	int outChunkLen, randomxRoundCount, jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputData;
	ErlNifBinary inputChunk;

	if (argc != 8) {
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
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
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
		return error_tuple(envPtr, "failed to read state");
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
			return error_tuple(envPtr, "state has been released");
		}
		return error_tuple(envPtr, "randomx_create_vm failed");
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


ERL_NIF_INIT(ar_rx512_nif, rx512_funcs, rx512_load, NULL, NULL, NULL);

