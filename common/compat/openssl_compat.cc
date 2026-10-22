
#include <openssl/evp.h>
#include <openssl/ssl.h>

extern "C" {

int EVP_PKEY_get_base_id(const EVP_PKEY* pkey) {
    return EVP_PKEY_base_id(pkey);
}

int EVP_PKEY_get_bits(const EVP_PKEY* pkey) {
    return EVP_PKEY_bits(pkey);
}

X509* SSL_get1_peer_certificate(const SSL* ssl) {
    return SSL_get_peer_certificate(ssl);
}

}  // extern "C"
