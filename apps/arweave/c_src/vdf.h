#include <stdbool.h>
#include <gmp.h>
#include "randomx.h"

const int SALT_SIZE = 32;
const int VDF_SHA_HASH_SIZE = 32;

#if defined(__cplusplus)
extern "C" {
#endif

// out checkpoint should return all checkpoints including skipCheckpointCount
void vdf_sha2(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations);
bool vdf_parallel_sha_verify(unsigned char* startSaltBuffer, unsigned char* seed, int checkpointCount, int skipCheckpointCount, int hashingIterations, unsigned char* inRes, unsigned char* inCheckpoint, unsigned char* outCheckpoint, int maxThreadCount);
bool vdf_parallel_sha_verify_with_reset(unsigned char* startSaltBuffer, unsigned char* seed, int checkpointCount, int skipCheckpointCount, int hashingIterations, unsigned char* inRes, unsigned char* inCheckpoint, unsigned char* outCheckpoint, unsigned char* resetSalt, unsigned char* resetSeed, int maxThreadCount);

#if defined(__cplusplus)
}
#endif
