#ifndef PACK_RANDOMX_SQUARE_H
#define PACK_RANDOMX_SQUARE_H

#include "randomx.h"

#if defined(__cplusplus)
extern "C" {
#endif

RANDOMX_EXPORT void randomx_squared_exec(
    randomx_vm *machine, const unsigned char *inHash, const unsigned char *inScratchpad,
    unsigned char *outHash, unsigned char *outScratchpad, const int randomxProgramCount);
RANDOMX_EXPORT void randomx_squared_exec_test(
    randomx_vm *machine, const unsigned char *inHash, const unsigned char *inScratchpad,
    unsigned char *outHash, unsigned char *outScratchpad, const int randomxProgramCount);
// init_msg + hash
RANDOMX_EXPORT void randomx_squared_init_scratchpad(
    randomx_vm *machine, const unsigned char *input, const size_t inputSize,
    unsigned char *outHash, unsigned char *outScratchpad, const int randomxProgramCount);

RANDOMX_EXPORT void packing_mix_entropy_crc32(
    const unsigned char *inEntropy, unsigned char *outEntropy, const size_t entropySize);

RANDOMX_EXPORT void packing_mix_entropy_far(
    const unsigned char *inEntropy, unsigned char *outEntropy, const size_t entropySize,
    const size_t jumpSize, const size_t blockSize);

// TODO optimized packing_apply_to_subchunk (NIF only uses slice)

#if defined(__cplusplus)
}
#endif

#endif // PACK_RANDOMX_SQUARE_H
