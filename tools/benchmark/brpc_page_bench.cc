// Minimal page RPC baseline. Build explicitly with --target brpc_page_bench.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "common/config.h"
#include "common/rpc_transport.h"
#include "core/compute_node/compute_node.pb.h"

DEFINE_bool(serve, false, "Run the page server");
DEFINE_string(endpoint, "0.0.0.0:18624", "Server address:port");
DEFINE_bool(bench_rdma, true, "Use the project's bRPC RDMA transport");
DEFINE_int32(bytes, 4096, "Response payload bytes; use 0 for an empty-response control");
DEFINE_int32(warmup, 1000, "Unmeasured RPCs before sampling");
DEFINE_int32(iterations, 10000, "Measured RPCs, one outstanding request at a time");

class PageService : public compute_node_service::ComputeNodeService {
 public:
  explicit PageService(int bytes) : page_(bytes, 'p') {}

  void GetPage(google::protobuf::RpcController*,
               const compute_node_service::GetPageRequest*,
               compute_node_service::GetPageResponse* response,
               google::protobuf::Closure* done) override {
    brpc::ClosureGuard guard(done);
    response->set_page_data(page_);
  }

 private:
  const std::string page_;
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_bytes < 0 || FLAGS_warmup < 0 || FLAGS_iterations <= 0) {
    std::cerr << "Require bytes >= 0, warmup >= 0 and iterations > 0\n";
    return 1;
  }
  ConfigureRpcTransport(FLAGS_bench_rdma);

  if (FLAGS_serve) {
    PageService service(FLAGS_bytes);
    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) return 1;
    brpc::ServerOptions options;
    options.num_threads = 8;  // Same as the compute server.
    SET_BRPC_RDMA_OPTION(options, FLAGS_bench_rdma);
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) return 1;
    server.RunUntilAskedToQuit();
    return 0;
  }

  brpc::Channel channel;
  brpc::ChannelOptions options;
  SET_BRPC_RDMA_OPTION(options, FLAGS_bench_rdma);
  options.connect_timeout_ms = 1000;
  options.timeout_ms = 5000;
  options.max_retry = 0;
  if (channel.Init(FLAGS_endpoint.c_str(), &options) != 0) return 1;
  compute_node_service::ComputeNodeService_Stub stub(&channel);
  compute_node_service::GetPageRequest request;
  request.mutable_page_id()->set_table_id(1);
  request.mutable_page_id()->set_page_no(1);
  const std::string expected(FLAGS_bytes, 'p');
  std::vector<double> samples;
  samples.reserve(FLAGS_iterations);
  for (int64_t i = 0; i < int64_t(FLAGS_warmup) + FLAGS_iterations; ++i) {
    brpc::Controller controller;
    compute_node_service::GetPageResponse response;
    const auto start = std::chrono::steady_clock::now();
    stub.GetPage(&controller, &request, &response, nullptr);
    const auto end = std::chrono::steady_clock::now();
    if (controller.Failed()) {
      std::cerr << controller.ErrorText() << '\n';
      return 1;
    }
    if (response.page_data() != expected) {
      std::cerr << "Unexpected page response\n";
      return 1;
    }
    if (i >= FLAGS_warmup) {
      samples.push_back(std::chrono::duration<double, std::micro>(end - start).count());
    }
  }
  const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
  std::sort(samples.begin(), samples.end());
  const auto percentile = [&](double p) {
    return samples[static_cast<size_t>(std::ceil(p * samples.size())) - 1];
  };
  std::cout << "transport,bytes,iterations,mean_us,p50_us,p95_us,p99_us,min_us,max_us\n"
            << (FLAGS_bench_rdma ? "rdma" : "tcp") << ',' << FLAGS_bytes << ','
            << samples.size() << ',' << std::fixed << std::setprecision(3)
            << mean << ',' << percentile(0.50) << ',' << percentile(0.95) << ','
            << percentile(0.99) << ',' << samples.front() << ',' << samples.back() << '\n';
}
