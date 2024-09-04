#ifndef AR_RANDOMX_H
#define AR_RANDOMX_H

// From RandomX/src/jit_compiler.hpp
// needed for the JIT compiler to work on OpenBSD, NetBSD and Apple Silicon
#if defined(__OpenBSD__) || defined(__NetBSD__) || (defined(__APPLE__) && defined(__aarch64__))
#define RANDOMX_FORCE_SECURE
#endif

#endif // AR_RANDOMX_H