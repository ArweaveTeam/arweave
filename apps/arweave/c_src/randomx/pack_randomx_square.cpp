#include <cassert>
#include <chrono>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <openssl/sha.h>
#include <erl_nif.h>
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
		auto g_start = std::chrono::high_resolution_clock::now();
		// 1) Define the aligned struct for tempHash
		struct vm_hash_t {
			alignas(16) uint64_t tempHash[8]; // 64 bytes
		};

		// 2) Allocate the vm_hash_t array here in C++
		vm_hash_t* vmHashes = new (std::nothrow) vm_hash_t[2*laneCount];
		if (!vmHashes) {
			return 0; // indicates allocation failure
		}

		// 3) Initialize each VM scratchpad
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
				// Free memory and return error if hashing fails
				delete[] vmHashes;
				return 0;
			}
			fillAes1Rx4<false>(
				vmHashes[i].tempHash,
				scratchpadSize,
				(void*)vmList[i]->getScratchpad()
			);
		}

		// 4) Inline exec
		auto randomx_squared_exec_inplace = [&](randomx_vm* machine, uint64_t* srcTempHash, uint64_t* dstTempHash, int programCount, size_t scratchpadSize) {
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
		};

		// 5) Inline packing mix
		auto packing_mix_entropy_far_sets = [&](randomx_vm** inSet,
												randomx_vm** outSet,
												int count,
												size_t scratchpadSize,
												size_t jumpSize,
												size_t blockSize)
		{
			size_t totalSize = (size_t)count * scratchpadSize;  // total bytes across all lanes

			// A helper function to copy `length` bytes from global offset srcPos to dstPos in cross-lane memory.
			auto copyChunkCrossLane = [&](size_t srcPos, size_t dstPos, size_t length) {
				while (length > 0) {
					// Find source lane + offset
					int srcLane = (int)(srcPos / scratchpadSize);
					size_t offsetInSrcLane = srcPos % scratchpadSize;

					// Find destination lane + offset
					int dstLane = (int)(dstPos / scratchpadSize);
					size_t offsetInDstLane = dstPos % scratchpadSize;

					// How many bytes remain in source lane from offsetInSrcLane?
					size_t srcLaneRemain = scratchpadSize - offsetInSrcLane;
					// How many bytes remain in destination lane from offsetInDstLane?
					size_t dstLaneRemain = scratchpadSize - offsetInDstLane;

					// The chunk we can safely copy (without crossing a lane boundary)
					size_t chunkSize = length;
					if (chunkSize > srcLaneRemain) {
						chunkSize = srcLaneRemain;
					}
					if (chunkSize > dstLaneRemain) {
						chunkSize = dstLaneRemain;
					}

					// Perform the memcpy for this sub-chunk
					unsigned char* srcSp = (unsigned char*)(void*) inSet[srcLane]->getScratchpad();
					unsigned char* dstSp = (unsigned char*)(void*) outSet[dstLane]->getScratchpad();
					memcpy(dstSp + offsetInDstLane, srcSp + offsetInSrcLane, chunkSize);

					// Advance
					srcPos += chunkSize;
					dstPos += chunkSize;
					length -= chunkSize;
				}
			};

			// Now we replicate your leftover logic from the original packing_mix_entropy_far()
			size_t entropySize = totalSize;
			size_t numJumps = entropySize / jumpSize;
			size_t numBlocksPerJump = jumpSize / blockSize;
			size_t leftover = jumpSize % blockSize;

			size_t outOffset = 0;  // global offset in outSet
			for (size_t offset = 0; offset < numBlocksPerJump; ++offset) {
				for (size_t i = 0; i < numJumps; ++i) {
					size_t srcPos = i * jumpSize + offset * blockSize;  // global source offset
					copyChunkCrossLane(srcPos, outOffset, blockSize);
					outOffset += blockSize;
				}
			}

			if (leftover > 0) {
				for (size_t i = 0; i < numJumps; ++i) {
					size_t srcPos = i * jumpSize + numBlocksPerJump * blockSize;
					copyChunkCrossLane(srcPos, outOffset, leftover);
					outOffset += leftover;
				}
			}
		};

		// 6) Main depth iteration
		for (int d = 0; d < rxDepth; d++) {
			// Even iteration => run Set-A, mix -> Set-B
			for (int lane = 0; lane < laneCount; lane++) {
				randomx_squared_exec_inplace(vmList[lane], vmHashes[lane].tempHash, vmHashes[lane+laneCount].tempHash, randomxProgramCount, scratchpadSize);
			}
			packing_mix_entropy_far_sets(&vmList[0], &vmList[laneCount],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);

			d++; // second iteration in the pair
			if (d >= rxDepth) break;
			for (int lane = laneCount; lane < 2*laneCount; lane++) {
				randomx_squared_exec_inplace(vmList[lane], vmHashes[lane].tempHash, vmHashes[lane-laneCount].tempHash, randomxProgramCount, scratchpadSize);
			}
			packing_mix_entropy_far_sets(&vmList[laneCount], &vmList[0],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);
		}
		// NOTE still unoptimal. Last copy can be performed from scratchpad to output. But requires +1 variation (set to buffer)

		// 7) Copy final scratchpads into outAllScratchpads
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

		// 8) Free the vm_hash_t array
		delete[] vmHashes;

		{
			auto g_end = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double, std::milli> g_duration = g_end - g_start;
			printf("call %f\n", g_duration.count());
		}
		// If we made it here, success
		return 1;
	}



	struct vm_hash_t {
		alignas(16) uint64_t tempHash[8]; // 64 bytes
	};

	struct ThreadData {
		int laneIndex;
		randomx_vm** vmList;
		vm_hash_t* vmHashes;
		size_t scratchpadSize;
		const unsigned char* keyData;
		size_t keySize;
		std::vector<std::pair<int,int>>* g_srcDstIndex;
		int randomxProgramCount;
		// Add references to shared synchronization objects:
		std::mutex* mtx;
		std::condition_variable* cvMainToWorkers;
		std::condition_variable* cvWorkersToMain;
		int* i_iteration;
		volatile int o_iteration;
		bool* stopFlag;
	};

	static void randomx_squared_exec_inplace(
		randomx_vm* machine,
		uint64_t* srcTempHash,
		uint64_t* dstTempHash,
		int programCount,
		size_t scratchpadSz)
	{
		machine->resetRoundingMode();
		for (int chain = 0; chain < programCount - 1; chain++) {
			machine->run(srcTempHash);
			int br = randomx_blake2b(
				srcTempHash, 64,
				machine->getRegisterFile(), sizeof(randomx::RegisterFile),
				nullptr, 0
			);
			assert(br == 0);
		}
		// Final chain iteration
		machine->run(srcTempHash);
		int br = randomx_blake2b(
			dstTempHash, 64,
			machine->getRegisterFile(), sizeof(randomx::RegisterFile),
			nullptr, 0
		);
		assert(br == 0);

		// Single-lane post-processing
		packing_mix_entropy_crc32(
			(const unsigned char*)machine->getScratchpad(),
			(unsigned char*)machine->getScratchpad(),
			scratchpadSz
		);
	};

	static void packing_mix_entropy_far_sets(randomx_vm** inSet,
		randomx_vm** outSet,
		int count,
		size_t sPadSize,
		size_t jumpSize,
		size_t blkSize)
	{
		size_t totalSize = (size_t)count * sPadSize;
		auto copyChunkCrossLane = [&](size_t srcPos, size_t dstPos, size_t length)
		{
			while (length > 0) {
				int srcLane = (int)(srcPos / sPadSize);
				size_t offsetInSrcLane = srcPos % sPadSize;
				int dstLane = (int)(dstPos / sPadSize);
				size_t offsetInDstLane = dstPos % sPadSize;

				size_t srcLaneRemain = sPadSize - offsetInSrcLane;
				size_t dstLaneRemain = sPadSize - offsetInDstLane;

				size_t chunkSize = length;
				if (chunkSize > srcLaneRemain) {
					chunkSize = srcLaneRemain;
				}
				if (chunkSize > dstLaneRemain) {
					chunkSize = dstLaneRemain;
				}

				unsigned char* srcSp = (unsigned char*)inSet[srcLane]->getScratchpad();
				unsigned char* dstSp = (unsigned char*)outSet[dstLane]->getScratchpad();
				memcpy(dstSp + offsetInDstLane, srcSp + offsetInSrcLane, chunkSize);

				srcPos += chunkSize;
				dstPos += chunkSize;
				length -= chunkSize;
			}
		};

		size_t entropySize = totalSize;
		size_t numJumps = entropySize / jumpSize;
		size_t numBlocksPerJump = jumpSize / blkSize;
		size_t leftover = jumpSize % blkSize;

		size_t outOffset = 0;
		for (size_t offset = 0; offset < numBlocksPerJump; ++offset) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + offset * blkSize;
				copyChunkCrossLane(srcPos, outOffset, blkSize);
				outOffset += blkSize;
			}
		}
		if (leftover > 0) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + numBlocksPerJump * blkSize;
				copyChunkCrossLane(srcPos, outOffset, leftover);
				outOffset += leftover;
			}
		}
	}

	static void* worker_thread_func(void* arg) {
		ThreadData* td = (ThreadData*)arg;
		
		std::vector<std::pair<int,int>>& g_srcDstIndex = *td->g_srcDstIndex;
		// 1) Initialization step for each lane
		{
			unsigned char laneSeed[32];
			{
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, td->keyData, td->keySize);
				unsigned char laneIdxByte = (unsigned char)td->laneIndex + 1;
				SHA256_Update(&sha256, &laneIdxByte, 1);
				SHA256_Final(laneSeed, &sha256);
			}
			int blakeResult = randomx_blake2b(
				td->vmHashes[td->laneIndex].tempHash, sizeof(td->vmHashes[td->laneIndex].tempHash),
				laneSeed, 32,
				nullptr, 0
			);
			if (blakeResult != 0) {
				printf("randomx_blake2b fail\n");
			}
			fillAes1Rx4<false>(
				td->vmHashes[td->laneIndex].tempHash,
				td->scratchpadSize,
				(void*)td->vmList[td->laneIndex]->getScratchpad()
			);
		}

		for (int thread_iteration = 0; !*td->stopFlag; thread_iteration++) {
			{
				std::unique_lock<std::mutex> lock(*td->mtx);
				td->cvMainToWorkers->wait(lock, [&] {
					if (*td->stopFlag)
						return true;
					return (*td->i_iteration == thread_iteration);
				});
				if (*td->stopFlag) {
					break;
				}
			}

			int srcIdx = g_srcDstIndex[td->laneIndex].first;
			int dstIdx = g_srcDstIndex[td->laneIndex].second;
			// then:
			randomx_vm* vm	   = td->vmList[srcIdx];
			uint64_t*  srcHash   = td->vmHashes[srcIdx].tempHash;
			uint64_t*  dstHash   = td->vmHashes[dstIdx].tempHash;

			randomx_squared_exec_inplace(vm, srcHash, dstHash, td->randomxProgramCount, td->scratchpadSize);

			td->o_iteration = thread_iteration;
			td->cvWorkersToMain->notify_one();
		}
		return NULL;
	}

	int rsp_fused_entropy_low_latency(
		randomx_vm** vmList,
		size_t scratchpadSize,
		int replicaEntropySubChunkCount,	// not used in snippet, still in signature
		int compositePackingSubChunkSize,   // not used in snippet, still in signature
		int laneCount,
		int rxDepth,
		int randomxProgramCount,
		int blockSize,
		const unsigned char* keyData,
		size_t keySize,
		unsigned char* outAllScratchpads
	) {
		auto g_start = std::chrono::high_resolution_clock::now();

		// 2) Allocate the vm_hash_t array
		vm_hash_t* vmHashes = new (std::nothrow) vm_hash_t[2 * laneCount];
		if (!vmHashes) {
			return 0; // indicates allocation failure
		}

		// -------------------------------------------
		// PHASE 0: Parallel initialization of each VM scratchpad
		// We'll spawn laneCount threads and keep them alive for the entire function.
		// Each thread: 
		//	* does the SHA-256 lane seed
		//	* does randomx_blake2b of that seed into vmHashes[i].tempHash
		//	* calls fillAes1Rx4<false> into vmList[i]->getScratchpad()

		// We'll define a global vector for src/dst lane indices. Each iteration, the main thread sets them.
		// In real code, better to store them in a member of your object. For brevity, define it static here:
		std::vector<std::pair<int,int>> g_srcDstIndex(laneCount);

		// We'll store the lane thread objects:
		std::vector<ErlNifTid> threads;
		threads.reserve(laneCount);

		// Shared concurrency objects:
		std::mutex mtx;
		std::condition_variable cvMainToWorkers;
		std::condition_variable cvWorkersToMain;

		int iteration = -1;
		bool stopFlag = false;

		// Spawn threads
		std::vector<ThreadData> threadDataArr(laneCount);
		{
			ErlNifThreadOpts* threadOpts = NULL;
			for (int i = 0; i < laneCount; i++) {
				ThreadData& td = threadDataArr[i];
				td.laneIndex = i;
				td.vmList = vmList;
				td.vmHashes = vmHashes;
				td.scratchpadSize = scratchpadSize;
				td.keyData = keyData;
				td.keySize = keySize;
				td.g_srcDstIndex = &g_srcDstIndex;
				td.randomxProgramCount = randomxProgramCount;
				td.mtx = &mtx;
				td.cvMainToWorkers = &cvMainToWorkers;
				td.cvWorkersToMain = &cvWorkersToMain;
				td.i_iteration = &iteration;
				td.o_iteration = iteration;
				td.stopFlag = &stopFlag;
				
				if (enif_thread_create(
					const_cast<char*>("worker_thread"),
					&threads[i],
					worker_thread_func,
					(void*)&td,
					threadOpts
				) != 0) {
					printf("!enif_thread_create\n");
					return 0;
				}
			}
		}

		// -------------------------------------------
		// Helper for parallel “run step”:
		auto parallel_run_step = [&]() {
			{
				std::unique_lock<std::mutex> lock(mtx);
				iteration++;
			}
			cvMainToWorkers.notify_all();

			// Wait until all lanes are done. The simplest approach is to spin up a small counter or wait for each lane.
			// For brevity, we’ll do something like this:
			{
				for (int i = 0; i < laneCount; i++) {
					if (iteration == threadDataArr[i].o_iteration) continue;
					std::unique_lock<std::mutex> lock(mtx);
					cvWorkersToMain.wait(lock, [&] {
						return iteration == threadDataArr[i].o_iteration;
					});
				}
			}
		};

		// -------------------------------------------
		// 6) Main depth iteration logic
		for(int d = 0;d < rxDepth;d++) {
			for (int lane = 0; lane < laneCount; lane++) {
				g_srcDstIndex[lane] = { lane, lane + laneCount };
			}
			parallel_run_step();

			// single-threaded packing
			packing_mix_entropy_far_sets(&vmList[0], &vmList[laneCount],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);

			d++;
			if (d >= rxDepth) break;

			for (int lane = 0; lane < laneCount; lane++) {
				g_srcDstIndex[lane] = { lane + laneCount, lane };
			}
			parallel_run_step();
			packing_mix_entropy_far_sets(&vmList[laneCount], &vmList[0],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);
		}

		// -------------------------------------------
		// 7) Copy final scratchpads into outAllScratchpads
		{
			if ((rxDepth % 2) == 0) {
				unsigned char* outAllScratchpadsPtr = outAllScratchpads;
				for (int i = 0; i < laneCount; i++) {
					void* sp = (void*)vmList[i]->getScratchpad();
					memcpy(outAllScratchpadsPtr, sp, scratchpadSize);
					outAllScratchpadsPtr += scratchpadSize;
				}
			} else {
				unsigned char* outAllScratchpadsPtr = outAllScratchpads;
				for (int i = laneCount; i < 2 * laneCount; i++) {
					void* sp = (void*)vmList[i]->getScratchpad();
					memcpy(outAllScratchpadsPtr, sp, scratchpadSize);
					outAllScratchpadsPtr += scratchpadSize;
				}
			}
		}

		// -------------------------------------------
		// Signal threads to stop, then join
		{
			std::unique_lock<std::mutex> lock(mtx);
			stopFlag = true;
		}
		cvMainToWorkers.notify_all(); // wake them up so they can exit

		// TODO don't wait
		for (auto& t : threads) {
			if (enif_thread_join(t, NULL) != 0) {
				printf("!enif_thread_join\n");
				return 0;
			}
		}

		// -------------------------------------------
		// 8) Free the vm_hash_t array
		delete[] vmHashes;
		
		{
			auto g_end = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double, std::milli> g_duration = g_end - g_start;
			printf("call %f\n", g_duration.count());
		}

		// If we made it here, success
		return 1;
	}


	// TODO optimized packing_apply_to_subchunk (NIF only uses slice)
}
