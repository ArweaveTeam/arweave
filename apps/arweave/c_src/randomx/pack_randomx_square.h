#ifndef PACK_RANDOMX_SQUARE_H
#define PACK_RANDOMX_SQUARE_H

#include "randomx.h"

#if defined(__cplusplus)
extern "C" {
#endif

RANDOMX_EXPORT void rsp_exec_test(
    randomx_vm *machine, const unsigned char *inHash, const unsigned char *inScratchpad,
    unsigned char *outHash, unsigned char *outScratchpad, const int randomxProgramCount);

RANDOMX_EXPORT void rsp_mix_entropy_crc32(
    const unsigned char *inEntropy, unsigned char *outEntropy, const size_t entropySize);

RANDOMX_EXPORT void rsp_mix_entropy_far(
    const unsigned char *inEntropy, unsigned char *outEntropy, const size_t entropySize,
    const size_t jumpSize, const size_t blockSize);

RANDOMX_EXPORT int rsp_fused_entropy(
    randomx_vm** vmList,
    size_t scratchpadSize,
    int replicaEntropySubChunkCount,
    int compositePackingSubChunkSize,
    int laneCount,
    int rxDepth,
    int randomxProgramCount,
    int blockSize,
    const unsigned char* keyData,
    size_t keySize,
    unsigned char* outAllScratchpads  // We'll pass in a pointer for final scratchpad data
);

// TODO optimized packing_apply_to_subchunk (NIF only uses slice)

#if defined(__cplusplus)
}
#endif

#endif // PACK_RANDOMX_SQUARE_H
