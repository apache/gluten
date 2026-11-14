set(VCPKG_BUILD_TYPE release)
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_C_FLAGS "-march=armv8-a+crc")
set(VCPKG_CXX_FLAGS "-march=armv8-a+crc -std=c++20")
set(VCPKG_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

# Do not compile a build-host CA bundle path into libcurl. With the default
# CURL_CA_BUNDLE=auto, curl's CMake picks the first CA bundle it finds on the
# build host (e.g. /etc/pki/tls/certs/ca-bundle.crt on CentOS/Rocky), and
# libcurl then fails with error 77 on runtime images where that path is
# missing. With nothing compiled in, the vcpkg curl port's CURL_CA_FALLBACK=ON
# makes libcurl use OpenSSL's default trust store, which honors SSL_CERT_FILE
# and SSL_CERT_DIR.
if("${PORT}" STREQUAL "curl")
    list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DCURL_CA_BUNDLE=none -DCURL_CA_PATH=none)
endif()

if("${PORT}" STREQUAL "openssl" AND "$ENV{VCPKG_DYNAMIC_OPENSSL}" STREQUAL "ON")
    set(VCPKG_LIBRARY_LINKAGE dynamic)
endif()
