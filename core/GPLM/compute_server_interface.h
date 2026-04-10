#pragma once
#include <brpc/channel.h>

class ComputeServerInterface {
public:
    virtual ~ComputeServerInterface() = default;
    virtual brpc::Channel* GetComputeChannel(int node_id) = 0;
    virtual void PushPageToOther(table_id_t table_id, page_id_t page_id, node_id_t dest_node_id, bool need_to_wait_log , bool from_global , int push_type) = 0;
    virtual int GetNodeID() = 0;
    virtual int Pending(page_id_t page_id, bool xpending, table_id_t table_id , node_id_t dest_node_id) = 0;
};
