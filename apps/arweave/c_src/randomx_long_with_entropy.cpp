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
	void randomx_calculate_hash_long(randomx_vm *machine, const unsigned char *input, const size_t inputSize, unsigned char *output, const int randomxProgramCount) {
		assert(machine != nullptr);
		assert(inputSize == 0 || input != nullptr);
		assert(output != nullptr);
		alignas(16) uint64_t tempHash[8];
		int blakeResult = blake2b(tempHash, sizeof(tempHash), input, inputSize, nullptr, 0);
		assert(blakeResult == 0);
		machine->initScratchpad(&tempHash);
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = blake2b(tempHash, sizeof(tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		machine->getFinalResult(output, RANDOMX_HASH_SIZE);
	}

	void randomx_calculate_hash_long_with_entropy(randomx_vm *machine, const unsigned char *input, const size_t inputSize, unsigned char *output, unsigned char *outputEntropy, const int randomxProgramCount) {
		assert(machine != nullptr);
		assert(inputSize == 0 || input != nullptr);
		assert(output != nullptr);
		alignas(16) uint64_t tempHash[8];
		int blakeResult = blake2b(tempHash, sizeof(tempHash), input, inputSize, nullptr, 0);
		assert(blakeResult == 0);
		machine->initScratchpad(&tempHash);
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = blake2b(tempHash, sizeof(tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		machine->getFinalResult(output, RANDOMX_HASH_SIZE);
		memcpy(outputEntropy, machine->getScratchpad(), RANDOMX_ENTROPY_SIZE);
	}

	const unsigned char *randomx_calculate_hash_long_with_entropy_get_entropy(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const int randomxProgramCount) {
		assert(machine != nullptr);
		assert(inputSize == 0 || input != nullptr);
		alignas(16) uint64_t tempHash[8];
		int blakeResult = blake2b(tempHash, sizeof(tempHash), input, inputSize, nullptr, 0);
		assert(blakeResult == 0);
		machine->initScratchpad(&tempHash);
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = blake2b(tempHash, sizeof(tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		unsigned char output[64];
		machine->getFinalResult(output, RANDOMX_HASH_SIZE);
		return (const unsigned char*)machine->getScratchpad();
	}

	void randomx_calculate_hash_long_with_entropy_first(randomx_vm* machine, const void* input, size_t inputSize) {
		blake2b(machine->tempHash, sizeof(machine->tempHash), input, inputSize, nullptr, 0);
		machine->initScratchpad(machine->tempHash);
	}

	void randomx_calculate_hash_long_with_entropy_next(randomx_vm* machine, const void* nextInput, size_t nextInputSize, void* output, void *outputEntropy, const int randomxProgramCount) {
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(machine->tempHash);
			blake2b(machine->tempHash, sizeof(machine->tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
		}
		machine->run(machine->tempHash);

		// Finish current hash and fill the scratchpad for the next hash at the same time
		blake2b(machine->tempHash, sizeof(machine->tempHash), nextInput, nextInputSize, nullptr, 0);
		memcpy(outputEntropy, machine->getScratchpad(), RANDOMX_ENTROPY_SIZE);
		machine->hashAndFill(output, RANDOMX_HASH_SIZE, machine->tempHash);
	}

	void randomx_calculate_hash_long_with_entropy_last(randomx_vm* machine, void* output, void *outputEntropy, const int randomxProgramCount) {
		machine->resetRoundingMode();
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(machine->tempHash);
			blake2b(machine->tempHash, sizeof(machine->tempHash), machine->getRegisterFile(), sizeof(randomx::RegisterFile), nullptr, 0);
		}
		machine->run(machine->tempHash);
		machine->getFinalResult(output, RANDOMX_HASH_SIZE);
		memcpy(outputEntropy, machine->getScratchpad(), RANDOMX_ENTROPY_SIZE);
	}

	// feistel_encrypt accepts padded message with 2*FEISTEL_BLOCK_LENGTH = 64 bytes
	RANDOMX_EXPORT void randomx_encrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize, unsigned char *outChunk, const int randomxProgramCount) {
		assert(inChunkSize <= RANDOMX_ENTROPY_SIZE);
		const unsigned char *outputEntropy = randomx_calculate_hash_long_with_entropy_get_entropy(machine, input, inputSize, randomxProgramCount);

		if (inChunkSize % (2*FEISTEL_BLOCK_LENGTH) == 0) {
			feistel_encrypt((const unsigned char*)inChunk, inChunkSize, outputEntropy, (unsigned char*)outChunk);
		} else {
			size_t inChunkSizeWithPadding = (((inChunkSize - 1) / (2*FEISTEL_BLOCK_LENGTH)) + 1)*2*FEISTEL_BLOCK_LENGTH;
			// unsigned char inChunkMax[256*1024] = {0}; // breaks app
			unsigned char *inChunkMax = (unsigned char*)malloc(inChunkSizeWithPadding);
			memcpy(inChunkMax, inChunk, inChunkSize);
			feistel_encrypt((const unsigned char*)inChunkMax, inChunkSizeWithPadding, outputEntropy, (unsigned char*)outChunk);
			free(inChunkMax);
		}
	}

	RANDOMX_EXPORT void randomx_decrypt_chunk(randomx_vm *machine, const unsigned char *input, const size_t inputSize, const unsigned char *inChunk, const size_t inChunkSize, unsigned char *outChunk, const int randomxProgramCount) {
		assert(inChunkSize <= RANDOMX_ENTROPY_SIZE);
		assert(inChunkSize % (2*FEISTEL_BLOCK_LENGTH) == 0);

		const unsigned char *outputEntropy = randomx_calculate_hash_long_with_entropy_get_entropy(machine, input, inputSize, randomxProgramCount);

		feistel_decrypt((const unsigned char*)inChunk, inChunkSize, outputEntropy, (unsigned char*)outChunk);
	}
}
