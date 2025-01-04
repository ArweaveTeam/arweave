#include <string.h>
#include <openssl/sha.h>
#include <ar_nif.h>
#include "../randomx_long_with_entropy.h"
#include "../feistel_msgsize_key_cipher.h"
#include "../randomx_squared.h"

#include "../ar_randomx_impl.h"

const int PACKING_KEY_SIZE = 32;
const int MAX_CHUNK_SIZE = 256*1024;

static int rxsquared_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info);
static ERL_NIF_TERM rxsquared_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rxsquared_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM rxsquared_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]);

static int rxsquared_load(ErlNifEnv* envPtr, void** priv, ERL_NIF_TERM info) {
	return load(envPtr, priv, info);
}

static ERL_NIF_TERM rxsquared_info_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	return info_nif("rxsquared", envPtr, argc, argv);
}

static ERL_NIF_TERM rxsquared_init_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	return init_nif(envPtr, argc, argv);
}

static ERL_NIF_TERM rxsquared_hash_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	return hash_nif(envPtr, argc, argv);
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

static ERL_NIF_TERM rsp_fused_entropy_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}

	// 1. Parse the state resource
	struct state* statePtr;
	if (!enif_get_resource(envPtr, argv[0], stateType, (void**)&statePtr)) {
		return error_tuple(envPtr, "failed_to_read_state");
	}

	// 2. Parse each integer
	int subChunkCount;
	if (!enif_get_int(envPtr, argv[1], &subChunkCount)) {
		return enif_make_badarg(envPtr);
	}

	int subChunkSize;
	if (!enif_get_int(envPtr, argv[2], &subChunkSize)) {
		return enif_make_badarg(envPtr);
	}

	int laneCount;
	if (!enif_get_int(envPtr, argv[3], &laneCount)) {
		return enif_make_badarg(envPtr);
	}

	int rxDepth;
	if (!enif_get_int(envPtr, argv[4], &rxDepth)) {
		return enif_make_badarg(envPtr);
	}

	int jitEnabled;
	if (!enif_get_int(envPtr, argv[5], &jitEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int largePagesEnabled;
	if (!enif_get_int(envPtr, argv[6], &largePagesEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int hardwareAESEnabled;
	if (!enif_get_int(envPtr, argv[7], &hardwareAESEnabled)) {
		return enif_make_badarg(envPtr);
	}

	int randomxProgramCount;
	if (!enif_get_int(envPtr, argv[8], &randomxProgramCount)) {
		return enif_make_badarg(envPtr);
	}

	// 3. Parse key as a binary
	ErlNifBinary keyBin;
	if (!enif_inspect_binary(envPtr, argv[9], &keyBin)) {
		return enif_make_badarg(envPtr);
	}

	// 4. Create VMs
	int totalVMs = 2 * laneCount;
	randomx_vm** vmList = (randomx_vm**)calloc(totalVMs, sizeof(randomx_vm*));
	if (!vmList) {
		return error_tuple(envPtr, "vmList_alloc_failed");
	}

	size_t scratchpadSize = randomx_get_scratchpad_size();

	// 5. Pre-allocate the final output binary to store all scratchpads
	size_t outEntropySize = scratchpadSize * laneCount;
	ERL_NIF_TERM outEntropyTerm;
	unsigned char* outEntropy =
		enif_make_new_binary(envPtr, outEntropySize, &outEntropyTerm);
	if (!outEntropy) {
		free(vmList);
		return enif_make_badarg(envPtr);
	}

	// 6. Create the randomx_vm objects
	int isRandomxReleased = 0;
	for (int i = 0; i < totalVMs; i++) {
		vmList[i] = create_vm(
			statePtr,
			(statePtr->mode == HASHING_MODE_FAST),
			jitEnabled,
			largePagesEnabled,
			hardwareAESEnabled,
			&isRandomxReleased
		);
		if (!vmList[i]) {
			// Clean up partial
			for (int j = 0; j < i; j++) {
				destroy_vm(statePtr, vmList[j]);
			}
			free(vmList);
			if (isRandomxReleased != 0) {
				return error_tuple(envPtr, "state_has_been_released");
			}
			return error_tuple(envPtr, "randomx_create_vm_failed");
		}
	}

	// 7. Call the pure C++ function that does the heavy logic and returns bool
	int success = rsp_fused_entropy(
		vmList,
		scratchpadSize,
		subChunkCount,
		subChunkSize,
		laneCount,
		rxDepth,
		randomxProgramCount,
		6,
		keyBin.data,
		keyBin.size,
		outEntropy  // final buffer for the output entropy
	);

	// 8. If the function returned false, we interpret that as an error
	if (!success) {
		// Cleanup
		for (int i = 0; i < totalVMs; i++) {
			if (vmList[i]) {
				destroy_vm(statePtr, vmList[i]);
			}
		}
		free(vmList);
		return error_tuple(envPtr, "cxx_fused_entropy_failed");
	}

	// 9. If success, destroy VMs and return {ok, outEntropyTerm}
	for (int i = 0; i < totalVMs; i++) {
		destroy_vm(statePtr, vmList[i]);
	}
	free(vmList);

	return ok_tuple(envPtr, outEntropyTerm);
}


static ErlNifFunc rxsquared_funcs[] = {
	{"rxsquared_info_nif", 1, rxsquared_info_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rxsquared_init_nif", 5, rxsquared_init_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rxsquared_hash_nif", 5, rxsquared_hash_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},

	{"rsp_fused_entropy_nif", 10,
		rsp_fused_entropy_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_feistel_encrypt_nif", 2, rsp_feistel_encrypt_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"rsp_feistel_decrypt_nif", 2, rsp_feistel_decrypt_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_rxsquared_nif, rxsquared_funcs, rxsquared_load, NULL, NULL, NULL);