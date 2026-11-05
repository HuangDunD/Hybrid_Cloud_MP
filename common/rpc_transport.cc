#include "rpc_transport.h"
#include "config.h"
#include <butil/logging.h>

void ConfigureRpcTransport(bool enabled) {
  use_rdma = enabled;
  if (use_rdma) {
    // Alibaba eRDMA Compat: IPv4 GID and up to four SGEs per send.
    // These flags exist only in a bRPC build with WITH_RDMA enabled.
    CHECK(!gflags::SetCommandLineOption("rdma_gid_index", "1").empty())
        << "use_rdma requires bRPC built with WITH_RDMA=ON";
    CHECK(!gflags::SetCommandLineOption("rdma_max_sge", "4").empty());
    CHECK(!gflags::SetCommandLineOption("rdma_prepared_qp_cnt", "0").empty());
  }
  LOG(INFO) << "RPC transport: " << (use_rdma ? "RDMA" : "TCP");
}
