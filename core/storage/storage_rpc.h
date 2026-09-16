#pragma once
#include <butil/logging.h> 
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>

#include "storage_service.pb.h"
#include "log_manager.h"
#include "disk_manager.h"
#include "record/rm_manager.h"
#include "common.h"
#include "sm_manager.h"

namespace storage_service{
class StoragePoolImpl : public StorageService{  
  public:
    StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, RmManager* rm_manager, brpc::Channel* raft_channels_, int raft_num , SmManager *sm_manager);

    virtual ~StoragePoolImpl();

    // 计算层向存储层写日志
    virtual void LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);
    
    // 写Raft日志
    virtual void RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);

    // 计算层向存储层读数据页
    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void GetPageWithLsn(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageWithLsnRequest* request,
                       ::storage_service::GetPageWithLsnResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void PrefetchIndex(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetBatchIndexRequest* request,
                       ::storage_service::GetBatchIndexResponse* response,
                       ::google::protobuf::Closure* done);

    // LJ
    virtual void WritePage(::google::protobuf::RpcController* controller,
                       const ::storage_service::WritePageRequest* request,
                       ::storage_service::WritePageResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done);
    virtual void DeletePage(::google::protobuf::RpcController *controller , 
                        const ::storage_service::DeletePageRequest *request ,
                        ::storage_service::DeletePageResponse *response ,
                        ::google::protobuf::Closure *done);

    // SQL
    virtual void OpenDb(::google::protobuf::RpcController* controller,
                       const ::storage_service::OpendbRequest* request,
                       ::storage_service::OpendbResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void TableExist(::google::protobuf::RpcController* controller,
                       const ::storage_service::TableExistRequest* request,
                       ::storage_service::TableExistResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void CreateTable(::google::protobuf::RpcController *controller,
                        const ::storage_service::CreateTableRequest *request ,
                        ::storage_service::CreateTableResponse *response ,
                      ::google::protobuf::Closure *done);
    virtual void DropTable(::google::protobuf::RpcController* controller,
                       const ::storage_service::DropTableRequest* request,
                       ::storage_service::DropTableResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void ShowTable(::google::protobuf::RpcController* controller,
                       const ::storage_service::ShowTableRequest* request,
                       ::storage_service::ShowTableResponse* response,
                       ::google::protobuf::Closure* done);

    // 故障通知
    virtual void NotifyNodeFailure(::google::protobuf::RpcController* controller,
                       const ::storage_service::StorageNodeFailureNotification* request,
                       ::storage_service::StorageNodeFailureAck* response,
                       ::google::protobuf::Closure* done);

    // 故障恢复第三阶段：日志分析
    virtual void AnalyzeRecoveryPages(::google::protobuf::RpcController* controller,
                       const ::storage_service::AnalyzeRecoveryPagesRequest* request,
                       ::storage_service::AnalyzeRecoveryPagesResponse* response,
                       ::google::protobuf::Closure* done);

  private:
    LogManager* log_manager_;
    DiskManager* disk_manager_;
    RmManager* rm_manager_;
    SmManager *sm_manager;
    brpc::Channel* raft_channels_;
    int raft_num_;

    std::mutex mutex;

    // ===== Instance Recovery: Undo 按恢复代数去重（支持多次顺序故障） =====
    // 旧实现用 static std::once_flag，进程生命周期只能触发一次：若同一存储进程运行期间
    // 发生第二次节点故障，UndoForFailedNode 将被永久跳过，第二个故障节点的未提交脏数据
    // 会永久残留。改为按"恢复代数"控制：每次收到新的故障通知递增 generation，
    // 同一代（同一次故障恢复）内多个存活节点的 AnalyzeRecoveryPages 请求只执行一次 Undo。
    std::mutex recovery_undo_mtx_;
    uint64_t recovery_generation_ = 0;      // 当前恢复代数（每个新的故障通知 +1）
    node_id_t recovery_failed_node_ = -1;   // 最近一次通知的故障节点（同一节点重复通知不递增）
    // 初始为 UINT64_MAX 表示"从未执行过 Undo"：不能初始化为 0，
    // 否则与 recovery_generation_ 的初始值相等，第一次故障时 Undo 会被错误跳过
    uint64_t undo_done_generation_ = UINT64_MAX;
    int shared_undo_count_ = 0;             // 本代 Undo 撤销的操作数（同代多个请求共享）
  };
}
