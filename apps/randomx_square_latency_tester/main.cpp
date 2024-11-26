#include <iostream>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include "pack_randomx_square.h"

int main() {
    return 0;

    // // Constants
    // const size_t entropySize = 8 * 1024 * 1024; // 8 MB
    // const int iterations = 100;

    // // Allocate memory for entropies
    // unsigned char* inEntropy = new unsigned char[entropySize];
    // unsigned char* keyEntropy = new unsigned char[entropySize];
    // unsigned char* outEntropy = new unsigned char[entropySize];

    // // Seed the random number generator
    // std::srand(static_cast<unsigned>(std::time(nullptr)));

    // // Fill entropies with random data
    // for (size_t i = 0; i < entropySize; ++i) {
    //     inEntropy[i] = std::rand() % 256;
    //     keyEntropy[i] = std::rand() % 256;
    // }

    // // Variables to store elapsed time
    // std::chrono::duration<double> elapsedFeistelShaFull(0);
    // std::chrono::duration<double> elapsedFeistelAesFull(0);
    // std::chrono::duration<double> elapsedFeistelCrc32(0);
    // std::chrono::duration<double> elapsedCrc32(0);
    // std::chrono::duration<double> elapsedFcrc32w(0);
    // std::chrono::duration<double> elapsedLcgMmix(0);
    // std::chrono::duration<double> elapsedSimdLcg(0);

    // // Benchmark packing_mix_entropy_feistel_sha_full
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_feistel_sha_full(inEntropy, keyEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedFeistelShaFull = endTime - startTime;
    // }
    // // Benchmark packing_mix_entropy_feistel_aes_full
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_feistel_aes_full(inEntropy, keyEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedFeistelAesFull = endTime - startTime;
    // }

    // // Benchmark packing_mix_entropy_feistel_crc32
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_feistel_crc32(inEntropy, keyEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedFeistelCrc32 = endTime - startTime;
    // }

    // // Benchmark packing_mix_entropy_fcrc32w
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_fcrc32w(inEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedFcrc32w = endTime - startTime;
    // }

    // // Benchmark packing_mix_entropy_crc32
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_crc32(inEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedCrc32 = endTime - startTime;
    // }

    // // Benchmark packing_mix_entropy_lcg_mmix
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_lcg_mmix(inEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedLcgMmix = endTime - startTime;
    // }

    // // Benchmark packing_mix_entropy_simd_lcg
    // {
    //     auto startTime = std::chrono::high_resolution_clock::now();

    //     for (int iter = 0; iter < iterations; ++iter) {
    //         // Modify the first byte of inEntropy
    //         inEntropy[0] = static_cast<unsigned char>((inEntropy[0] + 1) % 256);

    //         // Call the function
    //         packing_mix_entropy_simd_lcg(inEntropy, outEntropy, entropySize);
    //     }

    //     auto endTime = std::chrono::high_resolution_clock::now();
    //     elapsedSimdLcg = endTime - startTime;
    // }

    // // Output results
    // std::cout << "Benchmark results for " << iterations << " iterations on "
    //           << (entropySize / (1024 * 1024)) << " MB of data:\n\n";

    // std::cout << "1. packing_mix_entropy_feistel_sha_full:\n";
    // std::cout << "   Total time: " << elapsedFeistelShaFull.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedFeistelShaFull.count() / iterations) << " seconds.\n\n";

    // std::cout << "2. packing_mix_entropy_feistel_aes_full:\n";
    // std::cout << "   Total time: " << elapsedFeistelAesFull.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedFeistelAesFull.count() / iterations) << " seconds.\n\n";

    // std::cout << "3. packing_mix_entropy_feistel_crc32:\n";
    // std::cout << "   Total time: " << elapsedFeistelCrc32.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedFeistelCrc32.count() / iterations) << " seconds.\n\n";

    // std::cout << "4. packing_mix_entropy_crc32:\n";
    // std::cout << "   Total time: " << elapsedCrc32.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedCrc32.count() / iterations) << " seconds.\n\n";

    // std::cout << "5. packing_mix_entropy_fcrc32w:\n";
    // std::cout << "   Total time: " << elapsedFcrc32w.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedFcrc32w.count() / iterations) << " seconds.\n\n";

    // std::cout << "6. packing_mix_entropy_lcg_mmix:\n";
    // std::cout << "   Total time: " << elapsedLcgMmix.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedLcgMmix.count() / iterations) << " seconds.\n\n";

    // std::cout << "7. packing_mix_entropy_simd_lcg:\n";
    // std::cout << "   Total time: " << elapsedSimdLcg.count() << " seconds.\n";
    // std::cout << "   Average time per iteration: " << (elapsedSimdLcg.count() / iterations) << " seconds.\n\n";

    // // Clean up allocated memory
    // delete[] inEntropy;
    // delete[] keyEntropy;
    // delete[] outEntropy;

    // return 0;
}
