#include "randomx.h"

#define RANDOMX_ENTROPY_SIZE (256*1024)

#if defined(__cplusplus)
extern "C" {
#endif

RANDOMX_EXPORT void randomx_calculate_hash_long_with_entropy(randomx_vm *machine, const unsigned char *input, const size_t inputSize, unsigned char *output, unsigned char *outputEntropy, const int randomxProgramCount);
RANDOMX_EXPORT void randomx_calculate_hash_long_with_entropy_first(randomx_vm* machine, const void* input, size_t inputSize);
RANDOMX_EXPORT void randomx_calculate_hash_long_with_entropy_next(randomx_vm* machine, const void* nextInput, size_t nextInputSize, void* output, void *outputEntropy, const int randomxProgramCount);
RANDOMX_EXPORT void randomx_calculate_hash_long_with_entropy_last(randomx_vm* machine, void* output, void *outputEntropy, const int randomxProgramCount);

RANDOMX_EXPORT void randomx_encrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize,  unsigned char *outChunk, const int randomxProgramCount);
RANDOMX_EXPORT void randomx_decrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t outChunkSize, unsigned char *outChunk, const int randomxProgramCount);

#if defined(__cplusplus)
}
#endif
