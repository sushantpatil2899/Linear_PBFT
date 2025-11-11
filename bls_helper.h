#pragma once

#include <bls/bls384_256.h>
#include <bls/bls.hpp>
#include <string>
#include <vector>
#include <iostream>
#include <stdexcept>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>

namespace bls_utils {

inline void initBLS()
{
    static std::once_flag bls_once;
    std::call_once(bls_once, [] {
        bls::init();
    });
}

inline std::string binToHex(const std::vector<uint8_t>& v) {
    static const char* hexdigits = "0123456789abcdef";
    std::string out; out.reserve(v.size()*2);
    for (uint8_t b : v) {
        out.push_back(hexdigits[b>>4]);
        out.push_back(hexdigits[b&0xF]);
    }
    return out;
}

template<class T>
std::string toHexT(const T& x) {
    return x.serializeToHexStr();
}
template<class T>
T fromHexT(const std::string& s) {
    T x; x.clear();
    x.deserializeHexStr(s);
    return x;
}

inline std::string toHex(const bls::SecretKey& sk) { return toHexT(sk); }
inline std::string toHex(const bls::PublicKey& pk) { return toHexT(pk); }
inline std::string toHex(const bls::Signature& sg) { return toHexT(sg); }

inline bls::SecretKey skFromHex(const std::string& s) { return fromHexT<bls::SecretKey>(s); }
inline bls::PublicKey pkFromHex(const std::string& s) { return fromHexT<bls::PublicKey>(s); }
inline bls::Signature sigFromHex(const std::string& s) { return fromHexT<bls::Signature>(s); }

std::string computeDigest(const std::string& input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, input.c_str(), input.size());
    SHA256_Final(hash, &ctx);

    std::stringstream ss;
    for (unsigned char c : hash)
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)c;
    return ss.str();
}

inline std::string sign_data(const std::string& sk, const std::string& message)
{
    bls::SecretKey s = skFromHex(sk);
    std::string msgHash = computeDigest(message);
    bls::Signature sig;
    s.sign(sig, msgHash);
    return toHex(sig);
}

inline bool verify_signature(const std::string& pk, const std::string& sig, const std::string& message)
{
    bls::PublicKey p = pkFromHex(pk);
    bls::Signature sgn = sigFromHex(sig);
    std::string msgHash = computeDigest(message);
    return sgn.verify(p, msgHash);
}

inline std::string aggregate_signatures(const std::vector<std::string>& partialSigs,
                                          const std::vector<int>& ids)
{
    if (partialSigs.size() != ids.size() || partialSigs.empty()) {
        throw std::runtime_error("aggregate_signatures: mismatched or empty input");
    }
    std::vector<bls::Signature> partialSgns;
    std::vector<bls::Id> listIds;
    for (auto& sgn : partialSigs) {
        partialSgns.push_back(sigFromHex(sgn));
    }

    for (auto& id : ids) {
        listIds.push_back(id);
    }

    bls::Signature aggSig;
    aggSig.recover(partialSgns, listIds);
    return toHex(aggSig);
}

}