#include <string.h>
#include <openssl/sha.h>
#include <ar_nif.h>
#include "../randomx_long_with_entropy.h"
#include "../feistel_msgsize_key_cipher.h"
#include "../pack_randomx_square.h"

#include "../ar_randomx_impl.h"

const int PACKING_KEY_SIZE = 32;
const int MAX_CHUNK_SIZE = 256*1024;

static int rxsquared_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info);
static ERL_NIF_TERM rxsquared_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rxsquared_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rxsquared_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);

static int rxsquared_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info)
{
	return load(envPtr, priv, info);
}

static ERL_NIF_TERM rxsquared_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return info_nif("rxsquared", envPtr, argc, argv);
}

static ERL_NIF_TERM rxsquared_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return init_nif(envPtr, argc, argv);
}

static ERL_NIF_TERM rxsquared_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	return hash_nif(envPtr, argc, argv);
}

static ERL_NIF_TERM rsp_exec_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 7) {
		return enif_make_badarg(envPtr);
	}

	int randomxProgramCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inHashBin;
	ErlNifBinary inScratchpadBin;
	ERL_NIF_TERM outHashTerm;
	unsigned char* outHashData;
	ERL_NIF_TERM outScratchpadTerm;
	unsigned char* outScratchpadData;

	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error_tuple(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inHashBin)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inScratchpadBin)) {
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
	if (!enif_get_int(envPtr, argv[6], &randomxProgramCount)) {
		return enif_make_badarg(envPtr);
	}

	if (inHashBin.size != 64) {
		return enif_make_badarg(envPtr);
	}
	if (inScratchpadBin.size != randomx_get_scratchpad_size()) {
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

	outScratchpadData = enif_make_new_binary(
		envPtr, randomx_get_scratchpad_size(), &outScratchpadTerm);
	if (outScratchpadData == NULL) {
		return enif_make_badarg(envPtr);
	}

	outHashData = enif_make_new_binary(envPtr, 64, &outHashTerm);
	if (outHashData == NULL) {
		return enif_make_badarg(envPtr);
	}

	randomx_squared_exec(
		vmPtr, inHashBin.data, inScratchpadBin.data, outHashData, outScratchpadData,
		randomxProgramCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple2(envPtr, outHashTerm, outScratchpadTerm);
}

static ERL_NIF_TERM rsp_exec_test_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 7) {
		return enif_make_badarg(envPtr);
	}

	int randomxProgramCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inHashBin;
	ErlNifBinary inScratchpadBin;
	ERL_NIF_TERM outHashTerm;
	unsigned char* outHashData;
	ERL_NIF_TERM outScratchpadTerm;
	unsigned char* outScratchpadData;

	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error_tuple(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inHashBin)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[2], &inScratchpadBin)) {
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
	if (!enif_get_int(envPtr, argv[6], &randomxProgramCount)) {
		return enif_make_badarg(envPtr);
	}

	if (inHashBin.size != 64) {
		return enif_make_badarg(envPtr);
	}
	if (inScratchpadBin.size != randomx_get_scratchpad_size()) {
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

	outScratchpadData = enif_make_new_binary(
		envPtr, randomx_get_scratchpad_size(), &outScratchpadTerm);
	if (outScratchpadData == NULL) {
		return enif_make_badarg(envPtr);
	}

	outHashData = enif_make_new_binary(envPtr, 64, &outHashTerm);
	if (outHashData == NULL) {
		return enif_make_badarg(envPtr);
	}

	randomx_squared_exec_test(
		vmPtr, inHashBin.data, inScratchpadBin.data, outHashData, outScratchpadData,
		randomxProgramCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple2(envPtr, outHashTerm, outScratchpadTerm);
}

static ERL_NIF_TERM rsp_init_scratchpad_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 6) {
		return enif_make_badarg(envPtr);
	}

	int randomxProgramCount;
	int jitEnabled, largePagesEnabled, hardwareAESEnabled;
	struct state* statePtr;
	ErlNifBinary inputBin;
	ERL_NIF_TERM outHashTerm;
	unsigned char* outHashData;
	ERL_NIF_TERM outScratchpadTerm;
	unsigned char* outScratchpadData;

	if (!enif_get_resource(envPtr, argv[0], stateType, (void**) &statePtr)) {
		return error_tuple(envPtr, "failed to read state");
	}
	if (!enif_inspect_binary(envPtr, argv[1], &inputBin)) {
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
	if (!enif_get_int(envPtr, argv[5], &randomxProgramCount)) {
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

	outScratchpadData = enif_make_new_binary(
		envPtr, randomx_get_scratchpad_size(), &outScratchpadTerm);
	if (outScratchpadData == NULL) {
		return enif_make_badarg(envPtr);
	}

	outHashData = enif_make_new_binary(envPtr, 64, &outHashTerm);
	if (outHashData == NULL) {
		return enif_make_badarg(envPtr);
	}

	randomx_squared_init_scratchpad(
		vmPtr, inputBin.data, inputBin.size, outHashData, outScratchpadData,
		randomxProgramCount);

	destroy_vm(statePtr, vmPtr);

	return ok_tuple2(envPtr, outHashTerm, outScratchpadTerm);
}


// pack randomx square randomx independent
static ERL_NIF_TERM rsp_mix_entropy_crc32_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary inEntropyBin;
	ERL_NIF_TERM outEntropyTerm;
	unsigned char* outEntropyData;

	if (argc != 1) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[0], &inEntropyBin)) {
		return enif_make_badarg(envPtr);
	}

	size_t entropySize = inEntropyBin.size;

	if (entropySize % 8 != 0) {
		return enif_make_badarg(envPtr);
	}

	outEntropyData = enif_make_new_binary(envPtr, entropySize, &outEntropyTerm);
	if (outEntropyData == NULL) {
		return enif_make_badarg(envPtr);
	}

	packing_mix_entropy_crc32(inEntropyBin.data, outEntropyData, entropySize);

	return ok_tuple(envPtr, outEntropyTerm);
}

static ERL_NIF_TERM rsp_mix_entropy_far_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary inEntropyBin;
	ERL_NIF_TERM outEntropyTerm;
	unsigned char* outEntropyData;

	if (argc != 1) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[0], &inEntropyBin)) {
		return enif_make_badarg(envPtr);
	}

	size_t entropySize = inEntropyBin.size;

	if (entropySize % 8 != 0) {
		return enif_make_badarg(envPtr);
	}

	outEntropyData = enif_make_new_binary(envPtr, entropySize, &outEntropyTerm);
	if (outEntropyData == NULL) {
		return enif_make_badarg(envPtr);
	}

	packing_mix_entropy_far(inEntropyBin.data, outEntropyData, entropySize,
			randomx_get_scratchpad_size(), 6);

	return ok_tuple(envPtr, outEntropyTerm);
}

static ERL_NIF_TERM rsp_mix_entropy_far_test_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary inEntropyBin;
	ERL_NIF_TERM outEntropyTerm;
	unsigned char* outEntropyData;
	unsigned int jumpSize;
	unsigned int blockSize;

	if (argc != 3) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[0], &inEntropyBin)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[1], &jumpSize)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[2], &blockSize)) {
		return enif_make_badarg(envPtr);
	}

	size_t entropySize = inEntropyBin.size;

	if (entropySize % 8 != 0) {
		return enif_make_badarg(envPtr);
	}

	outEntropyData = enif_make_new_binary(envPtr, entropySize, &outEntropyTerm);
	if (outEntropyData == NULL) {
		return enif_make_badarg(envPtr);
	}

	packing_mix_entropy_far(inEntropyBin.data, outEntropyData, entropySize,
			jumpSize, blockSize);

	return ok_tuple(envPtr, outEntropyTerm);
}

static ERL_NIF_TERM rsp_feistel_encrypt_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary inMsgBin;
	ErlNifBinary inKeyBin;
	ERL_NIF_TERM outMsgTerm;
	unsigned char* outMsgData;

	if (argc != 2) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[0], &inMsgBin)) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[1], &inKeyBin)) {
		return enif_make_badarg(envPtr);
	}

	size_t msgSize = inMsgBin.size;

	if (inKeyBin.size != msgSize) {
		return enif_make_badarg(envPtr);
	}

	if (msgSize % 64 != 0) {
		return enif_make_badarg(envPtr);
	}

	outMsgData = enif_make_new_binary(envPtr, msgSize, &outMsgTerm);
	if (outMsgData == NULL) {
		return enif_make_badarg(envPtr);
	}

	feistel_encrypt(inMsgBin.data, msgSize, inKeyBin.data, outMsgData);

	return ok_tuple(envPtr, outMsgTerm);
}

static ERL_NIF_TERM rsp_feistel_decrypt_nif(
		ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary inMsgBin;
	ErlNifBinary inKeyBin;
	ERL_NIF_TERM outMsgTerm;
	unsigned char* outMsgData;

	if (argc != 2) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[0], &inMsgBin)) {
		return enif_make_badarg(envPtr);
	}

	if (!enif_inspect_binary(envPtr, argv[1], &inKeyBin)) {
		return enif_make_badarg(envPtr);
	}

	size_t msgSize = inMsgBin.size;

	if (inKeyBin.size != msgSize) {
		return enif_make_badarg(envPtr);
	}

	if (msgSize % 64 != 0) {
		return enif_make_badarg(envPtr);
	}

	outMsgData = enif_make_new_binary(envPtr, msgSize, &outMsgTerm);
	if (outMsgData == NULL) {
		return enif_make_badarg(envPtr);
	}

	feistel_decrypt(inMsgBin.data, msgSize, inKeyBin.data, outMsgData);

	return ok_tuple(envPtr, outMsgTerm);
}

static ErlNifFunc rxsquared_funcs[] = {
	{"rxsquared_info_nif", 1, rxsquared_info_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rxsquared_init_nif", 5, rxsquared_init_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rxsquared_hash_nif", 5, rxsquared_hash_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},

	{"rsp_exec_nif", 7,
		rsp_exec_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_exec_test_nif", 7,
		rsp_exec_test_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_init_scratchpad_nif", 6,
		rsp_init_scratchpad_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_mix_entropy_crc32_nif", 1,
		rsp_mix_entropy_crc32_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_mix_entropy_far_nif", 1,
		rsp_mix_entropy_far_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_mix_entropy_far_test_nif", 3,
		rsp_mix_entropy_far_test_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_feistel_encrypt_nif", 2, rsp_feistel_encrypt_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_feistel_decrypt_nif", 2, rsp_feistel_decrypt_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_rxsquared_nif, rxsquared_funcs, rxsquared_load, NULL, NULL, NULL);