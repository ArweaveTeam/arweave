#include <cassert>
#include "crc32.h"
#include "pack_randomx_square.h"
#include "feistel_msgsize_key_cipher.h"

// imports from randomx
#include "vm_compiled.hpp"
#include "blake2/blake2.h"
#include "aes_hash.hpp"

extern "C" {
	void randomx_squared_exec(
			randomx_vm *machine,
			const unsigned char *inHash, const unsigned char *inScratchpad,
			unsigned char *outHash, unsigned char *outScratchpad,
			const int randomxProgramCount) {
		assert(machine != nullptr);
		alignas(16) uint64_t tempHash[8];
		memcpy(tempHash, inHash, sizeof(tempHash));
		void* scratchpad = (void*)machine->getScratchpad();
		memcpy(scratchpad, inScratchpad, randomx_get_scratchpad_size());
		machine->resetRoundingMode();
		int blakeResult;
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = randomx_blake2b(
				tempHash, sizeof(tempHash), machine->getRegisterFile(),
				sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		
		blakeResult = randomx_blake2b(
			tempHash, sizeof(tempHash), machine->getRegisterFile(),
			sizeof(randomx::RegisterFile), nullptr, 0);
		assert(blakeResult == 0);
		
		memcpy(outHash, tempHash, sizeof(tempHash));

		packing_mix_entropy_crc32(
			(const unsigned char*)machine->getScratchpad(),
			outScratchpad, randomx_get_scratchpad_size());
	}

	void randomx_squared_exec_test(
			randomx_vm *machine,
			const unsigned char *inHash, const unsigned char *inScratchpad,
			unsigned char *outHash, unsigned char *outScratchpad,
			const int randomxProgramCount) {
		assert(machine != nullptr);
		alignas(16) uint64_t tempHash[8];
		memcpy(tempHash, inHash, sizeof(tempHash));
		void* scratchpad = (void*)machine->getScratchpad();
		memcpy(scratchpad, inScratchpad, randomx_get_scratchpad_size());
		machine->resetRoundingMode();
		int blakeResult;
		for (int chain = 0; chain < randomxProgramCount - 1; ++chain) {
			machine->run(&tempHash);
			blakeResult = randomx_blake2b(
				tempHash, sizeof(tempHash), machine->getRegisterFile(),
				sizeof(randomx::RegisterFile), nullptr, 0);
			assert(blakeResult == 0);
		}
		machine->run(&tempHash);
		
		blakeResult = randomx_blake2b(
			tempHash, sizeof(tempHash), machine->getRegisterFile(),
			sizeof(randomx::RegisterFile), nullptr, 0);
		assert(blakeResult == 0);
		
		memcpy(outHash, tempHash, sizeof(tempHash));
		memcpy(outScratchpad, machine->getScratchpad(), randomx_get_scratchpad_size());
	}

	// init_msg + hash
	void randomx_squared_init_scratchpad(
			randomx_vm *machine, const unsigned char *input, const size_t inputSize,
			unsigned char *outHash, unsigned char *outScratchpad,
			const int randomxProgramCount) {
		assert(machine != nullptr);
		assert(inputSize == 0 || input != nullptr);
		alignas(16) uint64_t tempHash[8];
		int blakeResult = randomx_blake2b(
			tempHash, sizeof(tempHash), input, inputSize, nullptr, 0);
		assert(blakeResult == 0);
		void* scratchpad = (void*)machine->getScratchpad();
		// bool softAes = false
		fillAes1Rx4<false>(tempHash, randomx_get_scratchpad_size(), scratchpad);
		
		memcpy(outHash, tempHash, sizeof(tempHash));
		memcpy(outScratchpad, machine->getScratchpad(), randomx_get_scratchpad_size());
	}

	void packing_mix_entropy_crc32(
			const unsigned char *inEntropy,
			unsigned char *outEntropy, const size_t entropySize) {
		// NOTE we can't use _mm_crc32_u64, because it output only final 32-bit result
		// NOTE commented variant is more readable but unoptimized
		unsigned int state = ~0;
		// unsigned int state = 0;
		const unsigned int *inEntropyPtr = (const unsigned int*)inEntropy;
		unsigned int *outEntropyPtr = (unsigned int*)outEntropy;
		for(size_t i=0;i<entropySize;i+=8) {
			//state = crc32(~state, *inEntropyPtr);
			//*outEntropyPtr = *inEntropyPtr ^ ~state;

			state = ~crc32(state, *inEntropyPtr);
			*outEntropyPtr = *inEntropyPtr ^ state;
			inEntropyPtr++;
			outEntropyPtr++;
			*outEntropyPtr = *inEntropyPtr;
			inEntropyPtr++;
			outEntropyPtr++;
		}

		// keep state
		// reset output entropy from start
		outEntropyPtr = (unsigned int*)outEntropy;
		outEntropyPtr++;
		// take input from output now
		inEntropyPtr = outEntropyPtr;
		// Note it's optimizeable now with only 1 pointer, but we will reduce readability later
		for(size_t i=0;i<entropySize;i+=8) {
			//state = crc32(~state, *inEntropyPtr);
			//*outEntropyPtr = *inEntropyPtr ^ ~state;

			state = ~crc32(state, *inEntropyPtr);
			*outEntropyPtr = *inEntropyPtr ^ state;
			inEntropyPtr += 2;
			outEntropyPtr += 2;
		}
	}

	void packing_mix_entropy_far(
			const unsigned char *inEntropy,
			unsigned char *outEntropy, const size_t entropySize,
			const size_t jumpSize, const size_t blockSize) {
		unsigned char *outEntropyPtr = outEntropy;
		size_t numJumps = entropySize / jumpSize;
		size_t numBlocksPerJump = jumpSize / blockSize;
		size_t leftover = jumpSize % blockSize;

		for (size_t offset = 0; offset < numBlocksPerJump; ++offset) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + offset * blockSize;
				memcpy(outEntropyPtr, &inEntropy[srcPos], blockSize);
				outEntropyPtr += blockSize;
			}
		}
		if (leftover > 0) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + numBlocksPerJump * blockSize;
				memcpy(outEntropyPtr, &inEntropy[srcPos], leftover);
				outEntropyPtr += leftover;
			}
		}
	}

	// TODO optimized packing_apply_to_subchunk (NIF only uses slice)
}
