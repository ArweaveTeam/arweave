#include <cassert>
#include <iostream>
#include <string>
#include <fstream>
#include <cstring>
#include <random>
#include <chrono>
#include <map>
#include "randomx.h"
#include "../randomx_long_with_entropy.h"

#define CHUNK_SIZE (256*1024)
#define RANDOMX_PACKING_ROUNDS_2_6 (8*45)
#define RANDOMX_PACKING_ROUNDS_2_5 (8*20)

// Helper functions copied from RandomX utility.hpp
constexpr char hexmap[] = "0123456789abcdef";
inline void outputHex(std::ostream& os, const uint8_t* data, int length) {
	for (int i = 0; i < length; ++i) {
		os << hexmap[(data[i] & 0xF0) >> 4];
		os << hexmap[data[i] & 0x0F];
	}
}

class Chunk {
    public:
        size_t size = CHUNK_SIZE;
        uint8_t* data = nullptr;

        Chunk() {
            this->data = new uint8_t[this->size];
        }
        ~Chunk() {
            delete[] this->data;
            this->data = nullptr;
        }
};

class RandomChunk : public Chunk {
    public:
        RandomChunk() : Chunk() {
            // Seed the random number generator
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 255);

            // Fill data with random bytes
            for (size_t i = 0; i < size; i++) {
                data[i] = static_cast<uint8_t>(dis(gen));
            }
        }
};


class Packer {
public:
    std::string key;
    randomx_flags initFlags = RANDOMX_FLAG_DEFAULT;
    randomx_flags packFlags = RANDOMX_FLAG_DEFAULT;
    randomx_cache *cachePtr = nullptr;
    randomx_dataset *datasetPtr = nullptr;

    Packer(
            const std::string& key,
            bool jitEnabled,
            bool largePagesEnabled,
            bool hardwareAESEnabled
    ) {
        // Since we're benchmarking encryption/decryption we assume FAST_HASHING_MODE rather
        // than the LIGHT_HASHING_MODE used for hashing
        this->key = key;
        
        if (jitEnabled) {
            this->initFlags |= RANDOMX_FLAG_JIT;
        }
        if (largePagesEnabled) {
            this->initFlags |= RANDOMX_FLAG_LARGE_PAGES;
        }
        this->packFlags = this->initFlags;
        this->packFlags |= RANDOMX_FLAG_FULL_MEM;
        if (hardwareAESEnabled) {
            this->packFlags |= RANDOMX_FLAG_HARD_AES;
        }
    }

    bool initialize() {
        this->cachePtr = randomx_alloc_cache(this->initFlags);
        randomx_init_cache(this->cachePtr, this->key.data(), this->key.size());
        this->datasetPtr = randomx_alloc_dataset(this->initFlags);
        // Assuming a single worker
        randomx_init_dataset(
            this->datasetPtr, this->cachePtr, randomx_dataset_item_count(), 0);
        randomx_release_cache(this->cachePtr);
        this->cachePtr = nullptr;
        return true;
    }

    randomx_vm* create_vm() {
        return randomx_create_vm(this->packFlags, this->cachePtr, this->datasetPtr);
    }

    void destroy_vm(randomx_vm* vm) {
        randomx_destroy_vm(vm);
    }

    void pack(randomx_vm* vm, int packing_rounds, const Chunk& input, Chunk& output) {
        randomx_encrypt_chunk(
            vm,
            reinterpret_cast<const unsigned char *>(this->key.data()), this->key.size(),
            reinterpret_cast<const unsigned char *>(input.data), input.size,
            reinterpret_cast<unsigned char *>(output.data), packing_rounds);
    }

    void unpack(randomx_vm* vm, int packing_rounds, const Chunk& input, Chunk& output) {
        randomx_decrypt_chunk(
            vm,
            reinterpret_cast<const unsigned char *>(this->key.data()), this->key.size(),
            reinterpret_cast<const unsigned char *>(input.data), input.size,
            reinterpret_cast<unsigned char *>(output.data), packing_rounds);
    }
};

class Harness {
public:
    Harness(size_t bytes_to_write, Packer* packer) {
        this->bytes_to_write = bytes_to_write;
        this->packer = packer;
        this->write_unpacked_data();
    }

    ~Harness() {
        delete this->packer;
        this->packer = nullptr;
    }

    void initialize_packer() {
        {
            Timer t(this, "init", "total");
            assert(this->packer->initialize());
        }
    }

    void run_vm_test() {
        int num_vms = 10000;
        std::cout << "Create and destroy " << num_vms << " VMs" << std::endl;
        {
            Timer t(this, "vm", std::to_string(num_vms));
            for (int i = 0; i < num_vms; i++) {
                randomx_vm* vm = this->packer->create_vm();
                assert(vm != 0);
                assert(vm != nullptr);
                this->packer->destroy_vm(vm);
            }
        }
    }

    void run_packing_2_6_no_reuse_test() {
        int num_chunks = 100;
        RandomChunk input;
        Chunk output;
        {
            Timer t(this, "packing_2_6_no_reuse", std::to_string(num_chunks));
            for (int i = 0; i < num_chunks; i++) {
                randomx_vm* vm = this->packer->create_vm();
                assert(vm != 0);
                assert(vm != nullptr);
                this->packer->pack(vm, RANDOMX_PACKING_ROUNDS_2_6, input, output);
                this->packer->destroy_vm(vm);
            }
        }
    }

     void run_packing_2_6_with_reuse_test() {
        int num_chunks = 100;
        RandomChunk input;
        Chunk output;
        randomx_vm* vm = this->packer->create_vm();
        assert(vm != 0);
        assert(vm != nullptr);
        {
            Timer t(this, "packing_2_6_with_reuse", std::to_string(num_chunks));
            for (int i = 0; i < num_chunks; i++) {
                this->packer->pack(vm, RANDOMX_PACKING_ROUNDS_2_6, input, output);
            }
        }
        this->packer->destroy_vm(vm);
    }

    void run_packing_2_5_no_reuse_test() {
        int num_chunks = 100;
        RandomChunk input;
        Chunk output;
        {
            Timer t(this, "packing_2_5_no_reuse", std::to_string(num_chunks));
            for (int i = 0; i < num_chunks; i++) {
                randomx_vm* vm = this->packer->create_vm();
                assert(vm != 0);
                assert(vm != nullptr);
                this->packer->pack(vm, RANDOMX_PACKING_ROUNDS_2_5, input, output);
                this->packer->destroy_vm(vm);
            }
        }
    }

     void run_packing_2_5_with_reuse_test() {
        int num_chunks = 100;
        RandomChunk input;
        Chunk output;
        randomx_vm* vm = this->packer->create_vm();
        assert(vm != 0);
        assert(vm != nullptr);
        {
            Timer t(this, "packing_2_5_with_reuse", std::to_string(num_chunks));
            for (int i = 0; i < num_chunks; i++) {
                this->packer->pack(vm, RANDOMX_PACKING_ROUNDS_2_5, input, output);
            }
        }
        this->packer->destroy_vm(vm);
    }

    void print_timings(int count) {
        for (const auto& outer : this->timing) {
            std::cout << outer.first << ":" << std::endl;
            for (const auto& inner : outer.second) {
                std::cout << "  " << inner.first << ": " << inner.second / count << " seconds" << std::endl;
            }
        }

    }

private:
    std::string unpacked_filename = "packing_benchmark.unpacked";
    std::string packed_A_filename = "packing_benchmark.packedA";
    std::string packed_B_filename = "packing_benchmark.packedB";
    size_t bytes_to_write = 0;
    Packer *packer = nullptr;

    std::map<std::string, std::map<std::string, double>> timing;

    class Timer {
        public:
            Timer(Harness* harness, const std::string& phase, const std::string& metric) {
                this->harness = harness;
                this->phase = phase;
                this->metric = metric;
                std::cout << phase << " " << metric << ": " << std::flush;
                this->start = std::chrono::high_resolution_clock::now();
            }

            ~Timer() {
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - this->start;
                this->harness->timing[phase][metric] += elapsed.count();
                std::cout << elapsed.count() << " seconds" << std::endl;
            }
        
        private:
            Harness* harness;
            std::string phase;
            std::string metric;
            std::chrono::high_resolution_clock::time_point start;
    };

    bool write_unpacked_data() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        // Write bytes_to_write bytes of random binary data to filename
        std::ofstream ofs(this->unpacked_filename, std::ios::binary);
        if (!ofs) {
            std::cerr << "Error: Could not open the file for writing." << std::endl;
            return false;
        }

        for (size_t i = 0; i < this->bytes_to_write; ++i) {
            char random_byte = static_cast<char>(dis(gen));
            ofs.write(&random_byte, sizeof(random_byte));
        }
        return true;
    }

};

int main() {
    std::cout << "Hello, world!" << std::endl;

    size_t bytes_to_write = 5 * 1024 * 1024; // 5 MB
    Packer *packer = new Packer("test key 000", true, true, true);
    Harness harness(bytes_to_write, packer);

    harness.initialize_packer();
    int count = 5;
    for (int i = 0; i < count; i++) {
        harness.run_vm_test();
        harness.run_packing_2_5_with_reuse_test();
        harness.run_packing_2_5_no_reuse_test();
        harness.run_packing_2_6_with_reuse_test();
        harness.run_packing_2_6_no_reuse_test();
    }
    std::cout << std::endl << "Average Timings" << std::endl;
    std::cout << "========" << std::endl;
    harness.print_timings(count);

    std::cout << "Done" << std::endl;
    return 0;
}