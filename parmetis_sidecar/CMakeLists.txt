# ParMETIS sidecar — separate executable, links MPI + ParMETIS.
# Built only when -DBUILD_PARMETIS_SIDECAR=ON is passed to cmake.
# This isolates MPI runtime from the main compute_server (which uses brpc).

cmake_minimum_required(VERSION 3.10)

find_package(MPI REQUIRED)
find_path(PARMETIS_INC NAMES parmetis.h)
find_library(PARMETIS_LIB NAMES parmetis)
find_library(METIS_LIB NAMES metis)

if(NOT PARMETIS_INC OR NOT PARMETIS_LIB OR NOT METIS_LIB)
    message(FATAL_ERROR "Failed to find ParMETIS / METIS. Install: apt install libparmetis-dev libmetis-dev")
endif()

add_executable(parmetis_sidecar
    main.cc
)

target_include_directories(parmetis_sidecar PRIVATE
    ${PARMETIS_INC}
    ${MPI_CXX_INCLUDE_DIRS}
)

target_link_libraries(parmetis_sidecar
    ${PARMETIS_LIB}
    ${METIS_LIB}
    ${MPI_CXX_LIBRARIES}
)

# ParMETIS uses 32-bit idx_t / float real_t by default — sidecar just consumes
# whatever the installed headers define; compute_server side mirrors via uds_protocol.h.
