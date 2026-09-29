set(VCPKG_BUILD_TYPE release)
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_C_FLAGS "-march=armv8-a+crc")
set(VCPKG_CXX_FLAGS "-march=armv8-a+crc -std=c++20")
set(VCPKG_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

if("${PORT}" STREQUAL "openssl" AND "$ENV{VCPKG_DYNAMIC_OPENSSL}" STREQUAL "ON")
    set(VCPKG_LIBRARY_LINKAGE dynamic)
endif()
