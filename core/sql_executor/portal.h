#pragma once

#include "vector"
#include "memory"

#include "sql_common.h"
#include "execution/ExecutorInsert.h"
#include "execution/ExecutorUpdate.h"
#include "execution/ExecutionManager.h"
#include "execution/ExecutionProjection.h"
#include "execution/ExecutorBPTree.h"
#include "execution/ExecutorSeqScan.h"
#include "execution/ExecutionDelete.h"
#include "execution/ExecutorJoin.h"


#include "optimizer/plan.h"

typedef enum portalTag{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt {
    portalTag tag;
    
    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;
    
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) :
            tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};


class Portal {
public:
    typedef std::shared_ptr<Portal> ptr;
    Portal(DTX *dtx_){
        dtx = dtx_;
    }

    ~Portal() = default;

    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->m_subPlan), x->m_selCols);
        }else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if (x->m_tag == T_SeqScan) {
                return std::make_unique<SeqScanExecutor>(dtx , x->m_tableName , x->m_filterConds);
            }else if (x->m_tag == T_BPTreeIndexScan) {
                return std::make_unique<BPTreeScanExecutor>(dtx, x->m_tableName, x->m_pointKey.value(), x->m_filterConds);
            }
            throw std::logic_error("Unsupported scan plan");
        } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->m_left);
            
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->m_right);
            std::unique_ptr<AbstractExecutor> join = std::make_unique<JoinExecutor>(
                                std::move(left),
                                std::move(right), x->m_conds,
                                dtx);
            return join;
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            // TODO
            throw std::logic_error("UnSupport Command");
        }
        return nullptr;
    }

    // 把查询计划给转化为整颗算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan , DTX *dtx) {
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY , std::vector<TabCol>() , std::unique_ptr<AbstractExecutor>() , plan);
        }else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        }else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch (x->m_tag) {
                case T_select : {
                    // Select 的 subplan 是一个投影
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->m_subPlan);
                    std::unique_ptr<AbstractExecutor> root= convert_plan_executor(p);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, p->m_selCols, std::move(root), plan);
                }
                case T_Update: {
                    // Update 的 subplan 是一个 scan，用来扫描要更新的数据
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->m_subPlan);
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<UpdateExecutor>(dtx , x->m_tabName , x->m_setClauses , std::move(scan));
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Delete: {
                    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->m_subPlan);
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<DeleteExecutor>(dtx , x->m_tabName , std::move(scan));
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Insert: {
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<InsertExecutor>(dtx, x->m_tabName , x->m_values);
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                default: {
                    throw LJ::UnixError();
                    break;
                }
            }
        }else {
            throw LJ::UnixError();
        }
        return nullptr;
    }


    run_stat run(std::shared_ptr<PortalStmt> portal , QlManager *ql , DTX *dtx) {
        switch (portal->tag) {
            case PORTAL_ONE_SELECT: {
                ql->select_from(std::move(portal->root) , std::move(portal->sel_cols) , dtx);
                return run_stat::NORMAL;
            }
            case PORTAL_DML_WITHOUT_SELECT: {
                ql->run_dml(std::move(portal->root));
                return run_stat::NORMAL;
            }
            case PORTAL_MULTI_QUERY: {
                ql->run_mutli_query(portal->plan);
                return run_stat::NORMAL;
            }
            case PORTAL_CMD_UTILITY: {
                return ql->run_cmd_utility(portal->plan);
            }
            default: {
                throw LJ::UnixError();
            }
        }
    }

private:
    DTX *dtx;
};