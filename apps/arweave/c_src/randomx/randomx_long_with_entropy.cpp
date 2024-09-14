#include <cassert>
#include "randomx_long_with_entropy.h"
#include "vm_interpreted.hpp"
#include "vm_interpreted_light.hpp"
#include "vm_compiled.hpp"
#include "vm_compiled_light.hpp"
#include "blake2/blake2.h"
#include "feistel_msgsize_key_cipher.h"

// NOTE. possible optimisation with outputEntropySize
// can improve performance for less memcpy (has almost no impact because randomx is too long 99+%)

extern "C" {
	const unsigned char *randomx_calculate_hash_long_with_entropy_get_entropy(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const int randomxProgramCount) {
		assert(machine != nullptr);
		assert(inputSize == 0 || input != nullptr);
		alignas(16) uint64_t tempHash[8];
		int blakeResult = randomx_blake2b(tempHash, sizeof(tempHash), input, inputSize, nullptr, 0);
		assert(blakeResult == 0);
		machine->initScratchpad(&tempHash);
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = randomx_blake2b(tempHash, sizeof(tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		unsigned char output[64];
		machine->getFinalResult(output, RANDOMX_HASH_SIZE);
		return (const unsigned char*)machine->getScratchpad();
	}

	// feistel_encrypt accepts padded message with 2*FEISTEL_BLOCK_LENGTH = 64 bytes
	RANDOMX_EXPORT void randomx_encrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize, unsigned char *outChunk, const int randomxProgramCount) {
		assert(inChunkSize <= RANDOMX_ENTROPY_SIZE);
		assert(inChunkSize % (2*FEISTEL_BLOCK_LENGTH) == 0);
		const unsigned char *outputEntropy = randomx_calculate_hash_long_with_entropy_get_entropy(machine, input, inputSize, randomxProgramCount);

		feistel_encrypt((const unsigned char*)inChunk, inChunkSize, outputEntropy, (unsigned char*)outChunk);
	}

	RANDOMX_EXPORT void randomx_decrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize, unsigned char *outChunk, const int randomxProgramCount) {
		assert(inChunkSize <= RANDOMX_ENTROPY_SIZE);
		assert(inChunkSize % (2*FEISTEL_BLOCK_LENGTH) == 0);

		const unsigned char *outputEntropy = randomx_calculate_hash_long_with_entropy_get_entropy(machine, input, inputSize, randomxProgramCount);

		feistel_decrypt((const unsigned char*)inChunk, inChunkSize, outputEntropy, (unsigned char*)outChunk);
	}
}
