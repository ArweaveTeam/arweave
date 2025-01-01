#ifndef RANDOMX_SQUARED_H
#define RANDOMX_SQUARED_H

#include "randomx.h"

#if defined(__cplusplus)
extern "C" {
#endif

RANDOMX_EXPORT int rsp_fused_entropy(
    randomx_vm** vmList,
    size_t scratchpadSize,
    int subChunkCount,
    int subChunkSize,
    int laneCount,
    int rxDepth,
    int randomxProgramCount,
    int blockSize,
    const unsigned char* keyData,
    size_t keySize,
    unsigned char* outEntropy  // We'll pass in a pointer for final scratchpad data
);

// TODO optimized packing_apply_to_subchunk (NIF only uses slice)

#if defined(__cplusplus)
}
#endif

#endif // RANDOMX_SQUARED_H
