#ifndef VDF_H
#define VDF_H

#include <stdbool.h>

const int SALT_SIZE = 32;
const int VDF_SHA_HASH_SIZE = 32;

static inline void long_add(unsigned char* saltBuffer, int checkpointIdx) {
	unsigned int acc = checkpointIdx;
	// big endian from erlang
	for(int i=SALT_SIZE-1;i>=0;i--) {
		unsigned int value = saltBuffer[i];
		value += acc;
		saltBuffer[i] = value & 0xFF;
		acc = value >> 8;
		if (acc == 0) break;
	}
}

#if defined(__cplusplus)
extern "C" {
#endif

// out checkpoint should return all checkpoints including skipCheckpointCount
void vdf_sha2(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations);
void vdf_sha2_fused_x86(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations);
void vdf_sha2_fused_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations);
void vdf_sha2_hiopt_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations);
bool vdf_parallel_sha_verify_with_reset(unsigned char* startSaltBuffer, unsigned char* seed, int checkpointCount, int skipCheckpointCount, int hashingIterations, unsigned char* inRes, unsigned char* inCheckpoint, unsigned char* outCheckpoint, unsigned char* resetSalt, unsigned char* resetSeed, int maxThreadCount);

#if defined(__cplusplus)
}
#endif


#endif
