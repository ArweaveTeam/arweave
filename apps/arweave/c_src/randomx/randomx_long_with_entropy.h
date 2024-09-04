#ifndef RANDOMX_LONG_WITH_ENTROPY_H
#define RANDOMX_LONG_WITH_ENTROPY_H

#include "randomx.h"

#define RANDOMX_ENTROPY_SIZE (256*1024)

#if defined(__cplusplus)
extern "C" {
#endif

RANDOMX_EXPORT void randomx_encrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize,  unsigned char *outChunk, const int randomxProgramCount);
RANDOMX_EXPORT void randomx_decrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t outChunkSize, unsigned char *outChunk, const int randomxProgramCount);

#if defined(__cplusplus)
}
#endif

#endif // RANDOMX_LONG_WITH_ENTROPY_H