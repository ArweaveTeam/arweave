#include <cassert>
#include <openssl/sha.h>
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


	void randomx_squared_exec_inplace(randomx_vm* machine, uint64_t* srcTempHash, uint64_t* dstTempHash, int programCount, size_t scratchpadSize) {
		machine->resetRoundingMode();
		for (int chain = 0; chain < programCount-1; chain++) {
			machine->run(srcTempHash);
			int br = randomx_blake2b(
				srcTempHash, 64,
				machine->getRegisterFile(),
				sizeof(randomx::RegisterFile),
				nullptr, 0
			);
			assert(br == 0);
		}
		machine->run(srcTempHash);
		int br = randomx_blake2b(
			dstTempHash, 64,
			machine->getRegisterFile(),
			sizeof(randomx::RegisterFile),
			nullptr, 0
		);
		assert(br == 0);
		packing_mix_entropy_crc32(
			(const unsigned char*)machine->getScratchpad(),
			(unsigned char*)(void*)machine->getScratchpad(),
			scratchpadSize);
	}

	void copyChunkCrossLane(
		randomx_vm** inSet,
		randomx_vm** outSet,
		size_t srcPos,
		size_t dstPos,
		size_t length,
		size_t scratchpadSize
	) {
		while (length > 0) {
			int srcLane = (int)(srcPos / scratchpadSize);
			size_t offsetInSrcLane = srcPos % scratchpadSize;

			int dstLane = (int)(dstPos / scratchpadSize);
			size_t offsetInDstLane = dstPos % scratchpadSize;

			size_t srcLaneRemain = scratchpadSize - offsetInSrcLane;
			size_t dstLaneRemain = scratchpadSize - offsetInDstLane;

			size_t chunkSize = length;
			if (chunkSize > srcLaneRemain) {
				chunkSize = srcLaneRemain;
			}
			if (chunkSize > dstLaneRemain) {
				chunkSize = dstLaneRemain;
			}

			unsigned char* srcSp = (unsigned char*)(void*) inSet[srcLane]->getScratchpad();
			unsigned char* dstSp = (unsigned char*)(void*) outSet[dstLane]->getScratchpad();
			memcpy(dstSp + offsetInDstLane, srcSp + offsetInSrcLane, chunkSize);

			srcPos += chunkSize;
			dstPos += chunkSize;
			length -= chunkSize;
		}
	}

	void packing_mix_entropy_far_sets(
		randomx_vm** inSet,
		randomx_vm** outSet,
		int count,
		size_t scratchpadSize,
		size_t jumpSize,
		size_t blockSize)
	{
		size_t totalSize = (size_t)count * scratchpadSize;

		size_t entropySize = totalSize;
		size_t numJumps = entropySize / jumpSize;
		size_t numBlocksPerJump = jumpSize / blockSize;
		size_t leftover = jumpSize % blockSize;

		size_t outOffset = 0;
		for (size_t offset = 0; offset < numBlocksPerJump; ++offset) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + offset * blockSize;
				copyChunkCrossLane(inSet, outSet, srcPos, outOffset, blockSize, scratchpadSize);
				outOffset += blockSize;
			}
		}

		if (leftover > 0) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + numBlocksPerJump * blockSize;
				copyChunkCrossLane(inSet, outSet, srcPos, outOffset, leftover, scratchpadSize);
				outOffset += leftover;
			}
		}
	}

	int rsp_fused_entropy(
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
		unsigned char* outAllScratchpads
	) {
		struct vm_hash_t {
			alignas(16) uint64_t tempHash[8]; // 64 bytes
		};

		vm_hash_t* vmHashes = new (std::nothrow) vm_hash_t[2*laneCount];
		if (!vmHashes) {
			return 0;
		}

		for (int i = 0; i < laneCount; i++) {
			unsigned char laneSeed[32];
			{
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, keyData, keySize);
				unsigned char laneIndex = (unsigned char)i + 1;
				SHA256_Update(&sha256, &laneIndex, 1);
				SHA256_Final(laneSeed, &sha256);
			}
			int blakeResult = randomx_blake2b(
				vmHashes[i].tempHash, sizeof(vmHashes[i].tempHash),
				laneSeed, 32,
				nullptr, 0
			);
			if (blakeResult != 0) {
				delete[] vmHashes;
				return 0;
			}
			fillAes1Rx4<false>(
				vmHashes[i].tempHash,
				scratchpadSize,
				(void*)vmList[i]->getScratchpad()
			);
		}

		for (int d = 0; d < rxDepth; d++) {
			for (int lane = 0; lane < laneCount; lane++) {
				randomx_squared_exec_inplace(vmList[lane], vmHashes[lane].tempHash, vmHashes[lane+laneCount].tempHash, randomxProgramCount, scratchpadSize);
			}
			packing_mix_entropy_far_sets(&vmList[0], &vmList[laneCount],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);

			if (d + 1 < rxDepth) {
				d++;
				for (int lane = laneCount; lane < 2*laneCount; lane++) {
					randomx_squared_exec_inplace(vmList[lane], vmHashes[lane].tempHash, vmHashes[lane-laneCount].tempHash, randomxProgramCount, scratchpadSize);
				}
				packing_mix_entropy_far_sets(&vmList[laneCount], &vmList[0],
											 laneCount, scratchpadSize, scratchpadSize,
											 blockSize);
			}
		}
		// NOTE still unoptimal. Last copy can be performed from scratchpad to output. But requires +1 variation (set to buffer)

		if ((rxDepth % 2) == 0) {
			unsigned char* outAllScratchpadsPtr = outAllScratchpads;
			for (int i = 0; i < laneCount; i++) {
				void* sp = (void*)vmList[i]->getScratchpad();
				memcpy(outAllScratchpadsPtr, sp, scratchpadSize);
				outAllScratchpadsPtr += scratchpadSize;
			}
		} else {
			unsigned char* outAllScratchpadsPtr = outAllScratchpads;
			for (int i = laneCount; i < 2*laneCount; i++) {
				void* sp = (void*)vmList[i]->getScratchpad();
				memcpy(outAllScratchpadsPtr, sp, scratchpadSize);
				outAllScratchpadsPtr += scratchpadSize;
			}
		}

		delete[] vmHashes;

		return 1;
	}

	// TODO optimized packing_apply_to_subchunk (NIF only uses slice)
}
