#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <iostream>
#include <brpc/channel.h>

#include "storage/storage_rpc.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "util/bench_control.h"

// Load DB
#include "smallbank/smallbank_db.h"
#include "tpcc/tpcc_db.h"
#include "ycsb/ycsb_db.h"
// #include "tpcc/tpcc_db.h"
// #include "ycsb/ycsb_db.h"

class Server {
public:
    Server(int machine_id, int local_rpc_port, int local_meta_port,
           bool use_rdma, int compute_node_num, std::vector<std::string> compute_ips, std::vector<int> compute_ports,
           DiskManager* disk_manager, LogManager* log_manager, RmManager* rm_manager, std::string workload):
        machine_id_(machine_id), local_rpc_port_(local_rpc_port), local_meta_port_(local_meta_port),
        compute_node_num_(compute_node_num), workload_(workload), disk_manager_(disk_manager),
        log_manager_(log_manager), rm_manager_(rm_manager), sm_manager(nullptr) {
        struct Auxiliary {
            DiskManager disk;
            LogManager log;
            storage_service::StoragePoolImpl impl;
            brpc::Server rpc;
            Auxiliary(int index, int port): log(&disk, nullptr, "Raft_Log" + std::to_string(index)),
                impl(&log, &disk, nullptr, nullptr, 0, nullptr) {
                if (rpc.AddService(&impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
                    throw std::runtime_error("raft service registration failed");
                brpc::ServerOptions options;
                if (rpc.Start(butil::EndPoint(butil::IP_ANY, port), &options) != 0)
                    throw std::runtime_error("raft service start failed");
            }
            ~Auxiliary() { rpc.Stop(0); rpc.Join(); }
        };
        const int raft_num = 2;
        std::vector<std::unique_ptr<Auxiliary>> auxiliary;
        raft_node_channels_ = new brpc::Channel[raft_num];
        brpc::ChannelOptions channel_options;
        channel_options.use_rdma = use_rdma;
        channel_options.timeout_ms = 30000;
        for (int i = 0; i < raft_num; ++i) {
            auxiliary.emplace_back(new Auxiliary(i, local_meta_port + 1 + i));
            const std::string address = "127.0.0.1:" + std::to_string(local_meta_port + 1 + i);
            if (raft_node_channels_[i].Init(address.c_str(), &channel_options) != 0)
                throw std::runtime_error("raft channel initialization failed");
        }
        brpc::Server server;
        storage_service::StoragePoolImpl impl(log_manager_, disk_manager_, rm_manager_, raft_node_channels_, raft_num, nullptr);
        if (workload == "ycsb")
            impl.RegisterComputeIndex("ycsb_user_table_bl", "ycsb_user_table_bl_compute");
        if (server.AddService(&impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            throw std::runtime_error("storage service registration failed");
        brpc::ServerOptions options;
        options.use_rdma = use_rdma;
        if (server.Start(butil::EndPoint(butil::IP_ANY, local_rpc_port), &options) != 0)
            throw std::runtime_error("storage service start failed");
        if (bench_control::enabled()) bench_control::publish("rpc-ready");
        SendMeta(machine_id, compute_node_num, workload);
        if (bench_control::enabled()) {
            bench_control::publish("ready");
            size_t checkpoint_index = 0;
            while (!bench_control::requested("drain")) {
                const std::string checkpoint = "checkpoint-" + std::to_string(checkpoint_index);
                if (bench_control::requested(checkpoint)) {
                    impl.FreezePhysicalWrites();
                    try {
                        log_manager_->SyncForValidation();
                        const auto checkpoint_cut = log_manager_->log_replay_->ValidationCut();
                        auto checkpoint_state = JsonConfig::empty_dict("storage-checkpoint");
                        checkpoint_state.insert_uint64("checkpoint", checkpoint_index);
                        checkpoint_state.insert_uint64("wal_tail_inclusive", checkpoint_cut.first);
                        checkpoint_state.insert_uint64("replay_inclusive", checkpoint_cut.second);
                        checkpoint_state.insert_uint64("active_undo_transactions", 0);
                        checkpoint_state.insert_bool("wal_checkpoint_fdatasync", true);
                        checkpoint_state.insert_bool("database_checkpoint_fdatasync", true);
                        checkpoint_state.insert_bool("physical_writes_frozen", true);
                        bench_control::publish(checkpoint, checkpoint_state);
                        bench_control::wait(checkpoint + ".release");
                    } catch (...) {
                        impl.ResumePhysicalWrites();
                        throw;
                    }
                    impl.ResumePhysicalWrites();
                    bench_control::publish(checkpoint + "-released");
                    ++checkpoint_index;
                    continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            server.Stop(0);
            server.Join();
            auxiliary.clear();
            log_manager_->SyncForValidation();
            const auto cut = log_manager_->log_replay_->ValidationCut();
            auto state = JsonConfig::empty_dict("storage-drain");
            state.insert_bool("database_final_fdatasync", true);
            state.insert_uint64("wal_tail_inclusive", cut.first);
            state.insert_uint64("replay_inclusive", cut.second);
            state.insert_uint64("active_undo_transactions", 0);
            state.insert_bool("rpc_stopped_and_joined", true);
            state.insert_bool("wal_final_fdatasync", true);
            bench_control::publish("drained", state);
            bench_control::wait("shutdown");
        } else {
            server.RunUntilAskedToQuit();
        }
        delete[] raft_node_channels_;
        raft_node_channels_ = nullptr;
    }

    Server(int machine_id, int local_rpc_port, int local_meta_port, 
                bool use_rdma, int compute_node_num, std::vector<std::string> compute_ips, std::vector<int> compute_ports,
                DiskManager* disk_manager, LogManager* log_manager, RmManager* rm_manager, SmManager *sm_manager_): 
          machine_id_(machine_id), local_rpc_port_(local_rpc_port), local_meta_port_(local_meta_port),
            compute_node_num_(compute_node_num), sm_manager(sm_manager_), disk_manager_(disk_manager), log_manager_(log_manager), rm_manager_(rm_manager)
        {   
            //初始化node channel
            int raft_num = 2;
            raft_node_channels_ = new brpc::Channel[raft_num];
            brpc::ChannelOptions channel_options;
            channel_options.use_rdma = use_rdma;
            channel_options.timeout_ms = 0x7FFFFFFF;
            for(int i = 0; i<raft_num; i++){
                // raft server
                std::thread rpc_thread([i, local_meta_port]{
                    brpc::Server server;
                    auto disk_manager = std::make_shared<DiskManager>();
                    auto log_manager = std::make_shared<LogManager>(disk_manager.get(), nullptr, "Raft_Log" + std::to_string(i));
                    storage_service::StoragePoolImpl raft_server_impl(log_manager.get(), disk_manager.get(), nullptr, nullptr, 0 , nullptr);
                    if (server.AddService(&raft_server_impl, 
                                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                        LOG(ERROR) << "Fail to add service";
                    }
                    butil::EndPoint point;
                    point = butil::EndPoint(butil::IP_ANY, local_meta_port+1+i);

                    brpc::ServerOptions options;
                    if (server.Start(point,&options) != 0) {
                        LOG(ERROR) << "Fail to start Server";
                    }
                    server.RunUntilAskedToQuit();
                });
                rpc_thread.detach();

                std::string ip = "127.0.0.1:" + std::to_string(local_meta_port+1+i);
                if(raft_node_channels_[i].Init(ip.c_str(), &channel_options) != 0) {
                    LOG(ERROR) << "Fail to init channel";
                    exit(1);
                }
            }

            //启动事务brpc server
            brpc::Server server;

            storage_service::StoragePoolImpl storage_pool_impl(log_manager_, disk_manager_, rm_manager_, raft_node_channels_, raft_num , sm_manager);
            if (server.AddService(&storage_pool_impl, 
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add service";
            }
            // 监听[0.0.0.0:local_port]
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, local_rpc_port);

            brpc::ServerOptions options;
            options.use_rdma = use_rdma;

            if (server.Start(point,&options) != 0) {
                LOG(ERROR) << "Fail to start Server";
            }
            std::cout << "Storage Server Start OK\n";

            server.RunUntilAskedToQuit();
    }

    ~Server() {}
    
    void SendMeta(node_id_t machine_id, size_t compute_node_num, std::string workload);

    void PrepareStorageMeta(node_id_t machine_id, std::string workload, char** hash_meta_buffer, size_t& total_meta_size);

    void SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size);

private:
    const int machine_id_;
    const int local_rpc_port_;
    const int local_meta_port_;
    
    int compute_node_num_;
    std::string workload_;
    DiskManager* disk_manager_;
    LogManager* log_manager_;
    RmManager* rm_manager_;
    SmManager *sm_manager;
    brpc::Channel* raft_node_channels_;
};
