#pragma once
#include <brpc/channel.h>
#include <vector>
#include <cstdint>

class ComputeServerInterface {
public:
    virtual ~ComputeServerInterface() = default;
    virtual brpc::Channel* GetComputeChannel(int node_id) = 0;
    virtual void PushPageToOther(table_id_t table_id, page_id_t page_id, node_id_t dest_node_id, bool need_to_wait_log , bool from_global , int push_type) = 0;
    virtual int GetNodeID() = 0;
    virtual int Pending(page_id_t page_id, bool xpending, table_id_t table_id , node_id_t dest_node_id) = 0;

    // 由 GPLM 在 LRPXLock 进入 xpending=true 分支前调用：向 holder_node 发起一次精检，
    // 询问目标槽位的 DataItem.lock 是否已经被其他事务 EXCLUSIVE_LOCKED。
    // - holder_node 与本节点相同：直接本地走 BufferPool；否则走 RPC。
    // - want_keys.empty()：跳过精检，返回 false（不冲突），保持原有流程。
    // - holder 不在 buffer 内或槽位 key 不匹配：返回 false（不阻塞优化的正确性）。
    virtual bool CheckPageTupleConflict(int holder_node,
                                        table_id_t table_id,
                                        page_id_t page_id,
                                        const std::vector<uint32_t>& want_slot_nos,
                                        const std::vector<uint64_t>& want_item_keys) = 0;
};
