// Wire format between compute_server and parmetis_sidecar.
// Length-prefix binary protocol — minimal, no third-party dependency.
//
// Same header is included from both:
//   - parmetis_sidecar/main.cc  (the sidecar process; links ParMETIS)
//   - core/affinity/partitioner.cc (the compute_server side; pure C++)
//
// idx_t / real_t mirror ParMETIS's typedefs:
//   - With default ParMETIS build:    idx_t = int32_t, real_t = float
//   - With -DIDXTYPEWIDTH=64:         idx_t = int64_t
//   - With -DREALTYPEWIDTH=64:        real_t = double
// We standardize on the 32-bit / float defaults — matches Debian/Ubuntu packages.
#pragma once

#include <cstdint>
#include <cstddef>

namespace affinity_uds {

using idx_t  = int32_t;
using real_t = float;

// Magic bytes for a sanity check at the start of each message.
constexpr uint32_t kReqMagic  = 0x57414654u;  // 'WAFT' (Wookong AFfinity request)
constexpr uint32_t kRespMagic = 0x57414653u;  // 'WAFS' (Wookong AFfinity response)

// Compute_server -> sidecar
//   uint32_t magic            = kReqMagic
//   uint32_t epoch            (monotonically increasing per partition cycle)
//   int32_t  nparts           (== ComputeNodeCount)
//   int32_t  ncon             (== 1)
//   int32_t  nvtx_local
//   int32_t  vtxdist_len      (== nparts + 1)
//   int32_t  xadj_len         (== nvtx_local + 1)
//   int32_t  adjncy_len
//   int32_t  has_vwgt         (0 or 1)
//   int32_t  has_vsize        (0 or 1)
//   int32_t  has_adjwgt       (0 or 1)
//   int32_t  has_prev_part    (0 or 1)
//   real_t   ubvec            (load balance tolerance, e.g. 1.05)
//   real_t   itr              (edgecut vs migration tradeoff)
//   idx_t[vtxdist_len]   vtxdist
//   idx_t[xadj_len]      xadj
//   idx_t[adjncy_len]    adjncy
//   (optional) idx_t[nvtx_local] vwgt    (only if has_vwgt)
//   (optional) idx_t[nvtx_local] vsize   (only if has_vsize)
//   (optional) idx_t[adjncy_len] adjwgt  (only if has_adjwgt)
//   (optional) idx_t[nvtx_local] prev_part (only if has_prev_part)
struct ReqHeader {
    uint32_t magic;
    uint32_t epoch;
    int32_t  nparts;
    int32_t  ncon;
    int32_t  nvtx_local;
    int32_t  vtxdist_len;
    int32_t  xadj_len;
    int32_t  adjncy_len;
    int32_t  has_vwgt;
    int32_t  has_vsize;
    int32_t  has_adjwgt;
    int32_t  has_prev_part;
    real_t   ubvec;
    real_t   itr;
};

// Sidecar -> compute_server
//   uint32_t magic   = kRespMagic
//   uint32_t epoch   (echoed)
//   int32_t  nvtx_local
//   int32_t  edgecut
//   int32_t  status  (0 = ok, non-zero = ParMETIS error code)
//   idx_t[nvtx_local] part
struct RespHeader {
    uint32_t magic;
    uint32_t epoch;
    int32_t  nvtx_local;
    int32_t  edgecut;
    int32_t  status;
};

}  // namespace affinity_uds
