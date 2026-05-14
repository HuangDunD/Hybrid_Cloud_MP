#pragma once
#include "common.h"
#include "config.h"
#include "GPLM/global_page_lock.h"
#include "GPLM/global_LR_page_lock.h"
#include "GPLM/global_partition_lock.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <cassert>
#include <brpc/channel.h>

class GlobalLockTable{ 
public:  
    GlobalLockTable(){
        compute_channels = new brpc::Channel*[MaxComputeNodeCount];
        for(int i=0; i<MaxComputeNodeCount; i++){
            compute_channels[i] = new brpc::Channel();
        }

        basic_page_table = new GlobalPageLock *[ComputeNodeBufferPageSize];
        for (int i = 0; i < ComputeNodeBufferPageSize; i++) {
            GlobalPageLock *lock = new GlobalPageLock(i);
            basic_page_table[i] = lock;
        }
        lr_page_table = new LR_GlobalPageLock *[ComputeNodeBufferPageSize];
        for (int i = 0; i < ComputeNodeBufferPageSize; i++) {
            LR_GlobalPageLock *lock = new LR_GlobalPageLock(i, compute_channels);
            lr_page_table[i] = lock;
        }
        partition_table = new GlobalPartionLock *[MaxPartitionCount];
        for (int i = 0; i < MaxPartitionCount; i++) {
            GlobalPartionLock *lock = new GlobalPartionLock(i);
            partition_table[i] = lock;
        }
    }

    // 最基础的锁
    GlobalPageLock* Basic_GetLock(page_id_t page_id){
        return basic_page_table[page_id];
    }

    LR_GlobalPageLock* LR_GetLock(page_id_t page_id) {
        return lr_page_table[page_id];
    }

    GlobalPartionLock* GetPartitionLock(partition_id_t partition_id){
        return partition_table[partition_id];
    }
    
    // 建立这张锁表到全部主节点的 RPC
    void BuildRPCConnection(std::vector<std::string> compute_node_ips, std::vector<int> compute_node_ports){
        brpc::ChannelOptions options;
        // options.timeout_ms = 5000; // 5秒超时
        options.timeout_ms = 0x7fffffff; // 2147483647ms
        options.use_rdma = use_rdma;
        options.connect_timeout_ms = 1000; // 1s
        options.max_retry = 10;
        assert(compute_node_ips.size() == (size_t)ComputeNodeCount);
        for(int i=0; i<ComputeNodeCount; i++){
            std::string ip = compute_node_ips[i];
            int port = compute_node_ports[i];
            std::string remote_node = ip + ":" + std::to_string(port);
            if(compute_channels[i]->Init(remote_node.c_str(), &options) != 0) {
                LOG(ERROR) << "Fail to init channel"; 
                exit(1);
            }
            else{
                // LOG(INFO) << "Connect to remote compute node " << i << " success";
            }
        }
    }

    void Reset(){
        if(basic_page_table != nullptr){
            for(int i=0; i<ComputeNodeBufferPageSize; i++){
                basic_page_table[i]->Reset();
            }
        }
        if(lr_page_table != nullptr){
            for(int i=0; i<ComputeNodeBufferPageSize; i++){
                lr_page_table[i]->Reset();
            }
        }
        if(partition_table != nullptr){
            for(int i=0; i<MaxPartitionCount; i++){
                partition_table[i]->Reset();
            }
        }
    }

    // Instance Recovery: 清理故障节点在此表所有页面上的状态
    // 对持有 X 锁的页面上 IR 锁（数据可能丢失）；S 锁仅清除 holder，不上 IR 锁
    // 返回受影响的页面数（分 X 锁和 S 锁）
    std::pair<int,int> CleanFailedNodeAndSetIRLock(node_id_t failed_node_id, GlobalValidTable* valid_table) {
        int x_affected = 0;
        int s_affected = 0;
        if (lr_page_table == nullptr) return {0, 0};
        for (int p = 0; p < ComputeNodeBufferPageSize; p++) {
            LR_GlobalPageLock* gl = lr_page_table[p];
            gl->mutexLock();
            int holder_type = gl->CleanFailedNodeNoBlock(failed_node_id);
            if (holder_type == 2) {
                // 故障节点持有 X 锁：最新数据可能在故障节点上，需要 IR 锁
                gl->SetIRLock();
                // 清理 GlobalValidInfo 中的故障节点
                GlobalValidInfo* vi = valid_table->GetValidInfo(p);
                vi->setNodeStatusNoBlock(failed_node_id, false);
                x_affected++;
            } else if (holder_type == 1) {
                // 故障节点仅持有 S 锁：其他 holder 仍有有效数据，不需要 IR 锁
                // 只需清理故障节点的有效性
                GlobalValidInfo* vi = valid_table->GetValidInfo(p);
                vi->setNodeStatusNoBlock(failed_node_id, false);
                s_affected++;
            }
            // holder_type == 0: 故障节点不是 holder，但仍需重置 pending/queue（已在 CleanFailedNodeNoBlock 中完成）
            gl->mutexUnlock();
        }
        return {x_affected, s_affected};
    }

    // Instance Recovery: 对此表所有页面设置 IR 锁（用于接管故障节点的 GPLM 时）
    void SetAllIRLocks() {
        if (lr_page_table == nullptr) return;
        for (int p = 0; p < ComputeNodeBufferPageSize; p++) {
            lr_page_table[p]->SetIRLock();
        }
    }

private:
    GlobalPageLock** basic_page_table = nullptr;
    LR_GlobalPageLock** lr_page_table = nullptr;
    GlobalPartionLock** partition_table = nullptr;

    brpc::Channel** compute_channels;
};

