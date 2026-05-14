// ParMETIS sidecar — one process per compute node.
// Launched via:
//   mpirun -np N --hostfile compute_hosts ./parmetis_sidecar /tmp/wookong_parmetis.sock
//
// Each sidecar:
//   1. Joins MPI_COMM_WORLD with the other N-1 sidecars.
//   2. Binds a local Unix domain socket; accepts one local compute_server.
//   3. Loops: recv a CSR slice -> ParMETIS_V3_AdaptiveRepart (collective) -> send part[].

#include <mpi.h>
#include <parmetis.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "uds_protocol.h"

namespace {
    std::string resolve_uds_path(const std::string &base_path, int rank, int size) {
        if (size <= 1) return base_path;
        return base_path + "." + std::to_string(rank);
    }

    int bind_uds(const std::string &path) {
        int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            std::fprintf(stderr, "socket() failed: %s\n", std::strerror(errno));
            return -1;
        }
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        if (path.size() >= sizeof(addr.sun_path)) {
            std::fprintf(stderr, "UDS path too long: %s\n", path.c_str());
            ::close(fd);
            return -1;
        }
        std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
        ::unlink(path.c_str());
        if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
            std::fprintf(stderr, "bind(%s) failed: %s\n", path.c_str(), std::strerror(errno));
            ::close(fd);
            return -1;
        }
        if (::listen(fd, 1) < 0) {
            std::fprintf(stderr, "listen failed: %s\n", std::strerror(errno));
            ::close(fd);
            return -1;
        }
        return fd;
    }

    bool read_full(int fd, void *buf, size_t n) {
        char *p = static_cast<char *>(buf);
        while (n > 0) {
            ssize_t r = ::read(fd, p, n);
            if (r == 0) return false;
            if (r < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            p += r;
            n -= r;
        }
        return true;
    }

    bool write_full(int fd, const void *buf, size_t n) {
        const char *p = static_cast<const char *>(buf);
        while (n > 0) {
            ssize_t w = ::write(fd, p, n);
            if (w < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            p += w;
            n -= w;
        }
        return true;
    }

    // Returns true on success, false on protocol/IO error (caller should
    // MPI_Abort because the partition cycle is collective).
    bool handle_one_request(int conn, int rank, int size) {
        affinity_uds::ReqHeader hdr{};
        if (!read_full(conn, &hdr, sizeof(hdr))) return false;
        if (hdr.magic != affinity_uds::kReqMagic) {
            std::fprintf(stderr, "[sidecar rank=%d] bad magic 0x%x\n", rank, hdr.magic);
            return false;
        }
        if (hdr.nparts != size) {
            std::fprintf(stderr,
                         "[sidecar rank=%d] nparts mismatch: hdr=%d MPI size=%d\n",
                         rank, hdr.nparts, size);
            return false;
        }

        std::vector<affinity_uds::idx_t> vtxdist(hdr.vtxdist_len);
        std::vector<affinity_uds::idx_t> xadj(hdr.xadj_len);
        std::vector<affinity_uds::idx_t> adjncy(hdr.adjncy_len);
        std::vector<affinity_uds::idx_t> vwgt, vsize, adjwgt, prev_part;

        if (!read_full(conn, vtxdist.data(),
                       vtxdist.size() * sizeof(affinity_uds::idx_t)))
            return false;
        if (!read_full(conn, xadj.data(),
                       xadj.size() * sizeof(affinity_uds::idx_t)))
            return false;
        if (!read_full(conn, adjncy.data(),
                       adjncy.size() * sizeof(affinity_uds::idx_t)))
            return false;
        if (hdr.has_vwgt) {
            vwgt.resize(hdr.nvtx_local);
            if (!read_full(conn, vwgt.data(),
                           vwgt.size() * sizeof(affinity_uds::idx_t)))
                return false;
        }
        if (hdr.has_vsize) {
            vsize.resize(hdr.nvtx_local);
            if (!read_full(conn, vsize.data(),
                           vsize.size() * sizeof(affinity_uds::idx_t)))
                return false;
        }
        if (hdr.has_adjwgt) {
            adjwgt.resize(hdr.adjncy_len);
            if (!read_full(conn, adjwgt.data(),
                           adjwgt.size() * sizeof(affinity_uds::idx_t)))
                return false;
        }
        if (hdr.has_prev_part) {
            prev_part.resize(hdr.nvtx_local);
            if (!read_full(conn, prev_part.data(),
                           prev_part.size() * sizeof(affinity_uds::idx_t)))
                return false;
        }

        // ParMETIS expects pointers — pass nullptr for omitted optional arrays.
        idx_t *p_vwgt = hdr.has_vwgt ? vwgt.data() : nullptr;
        idx_t *p_vsize = hdr.has_vsize ? vsize.data() : nullptr;
        idx_t *p_adjwgt = hdr.has_adjwgt ? adjwgt.data() : nullptr;

        long long local_directed_edge_weight = 0;
        if (hdr.has_adjwgt) {
            for (auto w : adjwgt) {
                local_directed_edge_weight += w;
            }
        } else {
            local_directed_edge_weight = hdr.adjncy_len;
        }
        long long global_directed_edge_weight = 0;
        MPI_Allreduce(&local_directed_edge_weight,
                      &global_directed_edge_weight, 1,
                      MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
        const int64_t total_edge_weight =
            static_cast<int64_t>(global_directed_edge_weight / 2);

        // AdaptiveRepart uses `part` as BOTH input (previous assignment) and output
        // (new assignment). If the caller provided prev_part, seed part[] with it —
        // otherwise ParMETIS sees "everything is in partition 0" and ends up doing
        // a full repartition every epoch, which defeats the "adaptive" bit entirely.
        std::vector<idx_t> part(hdr.nvtx_local, 0);
        if (hdr.has_prev_part &&
            prev_part.size() == static_cast<size_t>(hdr.nvtx_local)) {
            std::copy(prev_part.begin(), prev_part.end(), part.begin());
        }
        idx_t edgecut = 0;
        // Empty-graph epochs are valid early in a run before enough samples
        // accumulate to survive edge_min_weight pruning. Check the global
        // graph, because ParMETIS is collective and all ranks must agree.
        if (global_directed_edge_weight == 0) {
            affinity_uds::RespHeader rhdr{};
            rhdr.magic = affinity_uds::kRespMagic;
            rhdr.epoch = hdr.epoch;
            rhdr.nvtx_local = hdr.nvtx_local;
            rhdr.edgecut = 0;
            rhdr.total_edge_weight = total_edge_weight;
            rhdr.status = 0;
            if (!write_full(conn, &rhdr, sizeof(rhdr))) return false;
            if (rhdr.nvtx_local > 0 &&
                !write_full(conn, part.data(),
                            part.size() * sizeof(affinity_uds::idx_t))) {
                return false;
            }
            return true;
        }
        idx_t wgtflag = 0;
        if (hdr.has_vwgt) wgtflag |= 2;
        if (hdr.has_adjwgt) wgtflag |= 1;
        idx_t numflag = 0;
        idx_t ncon = hdr.ncon > 0 ? hdr.ncon : 1;
        idx_t nparts = hdr.nparts;
        std::vector<real_t> tpwgts(static_cast<size_t>(nparts) * static_cast<size_t>(ncon),
                                   1.0f / static_cast<float>(nparts));
        std::vector<real_t> ubvec(ncon, hdr.ubvec > 1.0f ? hdr.ubvec : 1.05f);
        real_t itr = hdr.itr > 0.0f ? hdr.itr : 1000.0f;
        idx_t options[4] = {0, 0, 0, 0};
        MPI_Comm comm = MPI_COMM_WORLD;

        int rc = ParMETIS_V3_AdaptiveRepart(
            vtxdist.data(), xadj.data(), adjncy.data(),
            p_vwgt, p_vsize, p_adjwgt,
            &wgtflag, &numflag, &ncon, &nparts,
            tpwgts.data(), ubvec.data(), &itr,
            options, &edgecut, part.data(), &comm);

        affinity_uds::RespHeader rhdr{};
        rhdr.magic = affinity_uds::kRespMagic;
        rhdr.epoch = hdr.epoch;
        rhdr.nvtx_local = hdr.nvtx_local;
        rhdr.edgecut = static_cast<int32_t>(edgecut);
        rhdr.total_edge_weight = total_edge_weight;
        rhdr.status = (rc == METIS_OK) ? 0 : rc;

        if (!write_full(conn, &rhdr, sizeof(rhdr))) return false;
        if (rhdr.nvtx_local > 0 &&
            !write_full(conn, part.data(),
                        part.size() * sizeof(affinity_uds::idx_t))) {
            return false;
        }
        return true;
    }
} // namespace

int main(int argc, char **argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <uds_path>\n", argv[0]);
        return 1;
    }
    MPI_Init(&argc, &argv);
    int rank = 0, size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    const std::string uds_path = resolve_uds_path(argv[1], rank, size);
    std::printf("[parmetis_sidecar rank=%d size=%d] up; uds=%s\n",
                rank, size, uds_path.c_str());

    int listen_fd = bind_uds(uds_path);
    if (listen_fd < 0) {
        MPI_Abort(MPI_COMM_WORLD, 2);
        return 2;
    }

    // Accept exactly one local compute_server. If it disconnects (compute_server
    // restart), accept again. The MPI collective state is reset implicitly: each
    // request from compute_server triggers one round of AdaptiveRepart.
    while (true) {
        std::printf("[parmetis_sidecar rank=%d] waiting for compute_server on %s\n",
                    rank, uds_path.c_str());
        int conn = ::accept(listen_fd, nullptr, nullptr);
        if (conn < 0) {
            std::fprintf(stderr, "accept failed: %s\n", std::strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 3);
            return 3;
        }
        std::printf("[parmetis_sidecar rank=%d] accepted compute_server\n", rank);

        // Request loop. Any IO/protocol error closes the connection and we
        // wait for the next compute_server. ParMETIS errors are returned via
        // RespHeader.status — no MPI_Abort because the collective itself
        // completed (just produced an error code).
        while (true) {
            if (!handle_one_request(conn, rank, size)) {
                break;
            }
        }
        ::close(conn);
        std::printf("[parmetis_sidecar rank=%d] compute_server disconnected\n", rank);
    }

    ::close(listen_fd);
    ::unlink(uds_path.c_str());
    MPI_Finalize();
    return 0;
}
