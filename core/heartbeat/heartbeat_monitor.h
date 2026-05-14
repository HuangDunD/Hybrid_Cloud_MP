#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <brpc/channel.h>
#include <butil/logging.h>

#include "compute_node/compute_node.pb.h"
#include "storage/storage_service.pb.h"
#include "common.h"

// 心跳监测器，运行在 Remote Server 上
// 负责监测所有计算节点的存活状态，故障时通知存活的计算节点和存储节点
class HeartbeatMonitor {
public:
    HeartbeatMonitor(const std::vector<std::string>& compute_ips,
                     const std::vector<int>& compute_ports,
                     const std::vector<std::string>& storage_ips,
                     const std::vector<int>& storage_ports,
                     int interval_ms = 1500,
                     int timeout_ms = 1000,
                     int max_retries = 3)
        : compute_ips_(compute_ips),
          compute_ports_(compute_ports),
          storage_ips_(storage_ips),
          storage_ports_(storage_ports),
          interval_ms_(interval_ms),
          timeout_ms_(timeout_ms),
          max_retries_(max_retries),
          running_(false) {

        int compute_count = compute_ips_.size();
        int storage_count = storage_ips_.size();

        // 初始化计算节点 channel
        compute_channels_.resize(compute_count);
        brpc::ChannelOptions options;
        options.timeout_ms = timeout_ms_;
        options.max_retry = 0;  // 心跳不自动重试，由监测逻辑自行控制
        options.connect_timeout_ms = timeout_ms_;
        options.connection_group = "heartbeat";  // 隔离心跳连接，避免与 GPLM channel 冲突

        for (int i = 0; i < compute_count; i++) {
            compute_channels_[i] = std::make_unique<brpc::Channel>();
            std::string addr = compute_ips_[i] + ":" + std::to_string(compute_ports_[i]);
            if (compute_channels_[i]->Init(addr.c_str(), &options) != 0) {
                LOG(ERROR) << "[HeartbeatMonitor] Failed to init channel to compute node " << i
                           << " (" << addr << ")";
            }
        }

        // 初始化存储节点 channel
        storage_channels_.resize(storage_count);
        brpc::ChannelOptions storage_options;
        storage_options.timeout_ms = 2000;
        storage_options.connect_timeout_ms = 1000;
        storage_options.max_retry = 1;
        storage_options.connection_group = "heartbeat";  // 隔离心跳连接

        for (int i = 0; i < storage_count; i++) {
            storage_channels_[i] = std::make_unique<brpc::Channel>();
            std::string addr = storage_ips_[i] + ":" + std::to_string(storage_ports_[i]);
            if (storage_channels_[i]->Init(addr.c_str(), &storage_options) != 0) {
                LOG(ERROR) << "[HeartbeatMonitor] Failed to init channel to storage node " << i
                           << " (" << addr << ")";
            }
        }

        // 初始化连续失败计数器和存活标志
        consecutive_failures_.resize(compute_count, 0);
        node_alive_.resize(compute_count, true);
    }

    ~HeartbeatMonitor() {
        Stop();
    }

    // 启动心跳监测（在所有计算节点就绪后调用）
    void Start() {
        running_.store(true);
        monitor_thread_ = std::thread(&HeartbeatMonitor::MonitorLoop, this);
        LOG(INFO) << "[HeartbeatMonitor] Started. interval=" << interval_ms_
                  << "ms, timeout=" << timeout_ms_
                  << "ms, max_retries=" << max_retries_;
        std::cout << "[HeartbeatMonitor] Heartbeat monitoring started for "
                  << compute_ips_.size() << " compute nodes." << std::endl;
    }

    // 停止心跳监测
    void Stop() {
        running_.store(false);
        if (monitor_thread_.joinable()) {
            monitor_thread_.join();
        }
        LOG(INFO) << "[HeartbeatMonitor] Stopped.";
    }

    // 检查某个节点是否存活
    bool IsNodeAlive(int node_id) const {
        if (node_id < 0 || node_id >= (int)node_alive_.size()) return false;
        return node_alive_[node_id];
    }

    // 注册故障回调，供外部扩展使用
    using FailureCallback = std::function<void(node_id_t failed_node_id)>;
    void RegisterFailureCallback(FailureCallback cb) {
        std::lock_guard<std::mutex> lk(cb_mutex_);
        failure_callbacks_.push_back(std::move(cb));
    }

private:
    // 心跳监测主循环
    void MonitorLoop() {
        while (running_.load()) {
            auto start = std::chrono::steady_clock::now();

            // 并发向所有存活的计算节点发送心跳
            ProbeAllComputeNodes();

            // 计算本轮耗时，精确控制间隔
            auto elapsed = std::chrono::steady_clock::now() - start;
            auto sleep_time = std::chrono::milliseconds(interval_ms_) - elapsed;
            if (sleep_time > std::chrono::milliseconds(0)) {
                std::this_thread::sleep_for(sleep_time);
            }
        }
    }

    // 向所有存活节点发送一轮心跳探测
    void ProbeAllComputeNodes() {
        int count = compute_ips_.size();

        // 使用 brpc 异步调用并行探测所有节点
        struct ProbeContext {
            brpc::Controller cntl;
            compute_node_service::HeartbeatRequest request;
            compute_node_service::HeartbeatResponse response;
            int node_id;
        };

        std::vector<ProbeContext> ctxs(count);

        // 发起所有异步 RPC
        for (int i = 0; i < count; i++) {
            if (!node_alive_[i]) continue;  // 已确认故障的节点不再探测

            ctxs[i].node_id = i;
            auto now = std::chrono::system_clock::now();
            auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            ctxs[i].request.set_timestamp(ts);

            compute_node_service::ComputeNodeService_Stub stub(compute_channels_[i].get());
            stub.Heartbeat(&ctxs[i].cntl, &ctxs[i].request, &ctxs[i].response, brpc::DoNothing());
        }

        // 等待所有 RPC 完成并检查结果
        for (int i = 0; i < count; i++) {
            if (!node_alive_[i]) continue;

            brpc::Join(ctxs[i].cntl.call_id());

            if (ctxs[i].cntl.Failed()) {
                consecutive_failures_[i]++;
                LOG(WARNING) << "[HeartbeatMonitor] Heartbeat to compute node " << i
                             << " failed (" << consecutive_failures_[i] << "/" << max_retries_
                             << "): " << ctxs[i].cntl.ErrorText();

                if (consecutive_failures_[i] >= max_retries_) {
                    OnNodeFailure(i);
                }
            } else {
                // 心跳成功，重置计数器
                if (consecutive_failures_[i] > 0) {
                    LOG(INFO) << "[HeartbeatMonitor] Compute node " << i << " recovered heartbeat.";
                }
                consecutive_failures_[i] = 0;
            }
        }
    }

    // 节点故障处理
    void OnNodeFailure(node_id_t failed_node_id) {
        node_alive_[failed_node_id] = false;

        auto now = std::chrono::system_clock::now();
        int64_t detection_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

        LOG(ERROR) << "[HeartbeatMonitor] *** Compute node " << failed_node_id
                   << " declared DEAD at timestamp " << detection_ts << " ***";
        std::cerr << "[HeartbeatMonitor] *** Compute node " << failed_node_id
                  << " declared DEAD ***" << std::endl;

        // 通知所有存活的计算节点
        NotifySurvivingComputeNodes(failed_node_id, detection_ts);

        // 通知所有存储节点
        NotifyStorageNodes(failed_node_id, detection_ts);

        // 执行注册的回调
        {
            std::lock_guard<std::mutex> lk(cb_mutex_);
            for (auto& cb : failure_callbacks_) {
                cb(failed_node_id);
            }
        }
    }

    // 通知所有存活的计算节点某个节点故障
    void NotifySurvivingComputeNodes(node_id_t failed_node_id, int64_t detection_ts) {
        int count = compute_ips_.size();

        struct NotifyCtx {
            brpc::Controller cntl;
            compute_node_service::NodeFailureNotification request;
            compute_node_service::NodeFailureAck response;
        };

        std::vector<NotifyCtx> ctxs(count);
        std::vector<brpc::CallId> call_ids;

        for (int i = 0; i < count; i++) {
            if (i == failed_node_id || !node_alive_[i]) continue;

            ctxs[i].request.set_failed_node_id(failed_node_id);
            ctxs[i].request.set_detection_timestamp(detection_ts);
            ctxs[i].cntl.set_timeout_ms(2000);

            compute_node_service::ComputeNodeService_Stub stub(compute_channels_[i].get());
            stub.NotifyNodeFailure(&ctxs[i].cntl, &ctxs[i].request, &ctxs[i].response, brpc::DoNothing());
            call_ids.push_back(ctxs[i].cntl.call_id());
        }

        // 等待所有通知完成
        for (auto& cid : call_ids) {
            brpc::Join(cid);
        }

        int notified = 0;
        for (int i = 0; i < count; i++) {
            if (i == failed_node_id || !node_alive_[i]) continue;
            if (!ctxs[i].cntl.Failed()) {
                notified++;
            } else {
                LOG(WARNING) << "[HeartbeatMonitor] Failed to notify compute node " << i
                             << " about failure of node " << failed_node_id
                             << ": " << ctxs[i].cntl.ErrorText();
            }
        }

        LOG(INFO) << "[HeartbeatMonitor] Notified " << notified
                  << " surviving compute nodes about failure of node " << failed_node_id;
    }

    // 通知所有存储节点某个计算节点故障
    void NotifyStorageNodes(node_id_t failed_node_id, int64_t detection_ts) {
        int count = storage_ips_.size();
        if (count == 0) return;

        struct NotifyCtx {
            brpc::Controller cntl;
            storage_service::StorageNodeFailureNotification request;
            storage_service::StorageNodeFailureAck response;
        };

        std::vector<NotifyCtx> ctxs(count);
        std::vector<brpc::CallId> call_ids;

        for (int i = 0; i < count; i++) {
            ctxs[i].request.set_failed_node_id(failed_node_id);
            ctxs[i].request.set_detection_timestamp(detection_ts);
            ctxs[i].cntl.set_timeout_ms(2000);

            storage_service::StorageService_Stub stub(storage_channels_[i].get());
            stub.NotifyNodeFailure(&ctxs[i].cntl, &ctxs[i].request, &ctxs[i].response, brpc::DoNothing());
            call_ids.push_back(ctxs[i].cntl.call_id());
        }

        for (auto& cid : call_ids) {
            brpc::Join(cid);
        }

        int notified = 0;
        for (int i = 0; i < count; i++) {
            if (!ctxs[i].cntl.Failed()) {
                notified++;
            } else {
                LOG(WARNING) << "[HeartbeatMonitor] Failed to notify storage node " << i
                             << " about failure of node " << failed_node_id
                             << ": " << ctxs[i].cntl.ErrorText();
            }
        }

        LOG(INFO) << "[HeartbeatMonitor] Notified " << notified
                  << " storage nodes about failure of node " << failed_node_id;
    }

private:
    // 配置
    std::vector<std::string> compute_ips_;
    std::vector<int> compute_ports_;
    std::vector<std::string> storage_ips_;
    std::vector<int> storage_ports_;
    int interval_ms_;
    int timeout_ms_;
    int max_retries_;

    // brpc channels
    std::vector<std::unique_ptr<brpc::Channel>> compute_channels_;
    std::vector<std::unique_ptr<brpc::Channel>> storage_channels_;

    // 状态
    std::vector<int> consecutive_failures_;
    std::vector<bool> node_alive_;

    // 监测线程
    std::atomic<bool> running_;
    std::thread monitor_thread_;

    // 回调
    std::mutex cb_mutex_;
    std::vector<FailureCallback> failure_callbacks_;
};
