#include "ExecutionManager.h"
#include "common.h"

void QlManager::run_mutli_query(std::shared_ptr<Plan> plan){
    run_res.clear();
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->m_tag) {
            case T_CreateTable: {
                run_res = "success";
                compute_server->create_table(x->m_tabName , x->m_cols , x->m_pkey);
                break;
            }
            case T_CreateIndex: {
                throw std::logic_error("Unsupport Command");
            }
            case T_DropTable: {
                compute_server->dropTable(x->m_tabName);
                run_res = "success";
                break;
            }
            case T_DropIndex: {
                throw std::logic_error("Unsupport Command");
            }
            default: {
                throw LJ::InternalError("Error");
                break;
            }
        }
    }
}

run_stat QlManager::run_cmd_utility(std::shared_ptr<Plan> plan){
    run_res.clear();
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->m_tag) {
            case T_Help: {
                // 不支持
                throw std::logic_error("UnSupport Cmd");
                assert(false);
            }
            case T_ShowTable: {
                run_res = compute_server->show_tables();
                return run_stat::NORMAL;
            }
            case T_DescTable: {
                run_res = compute_server->desc_table(x->m_tabName);
                return run_stat::NORMAL;
            }
            case T_Transaction_begin:{
                run_res = "Begin";
                return run_stat::TXN_BEGIN;
            }
            case T_Transaction_commit: {
                run_res = "Commit";
                return run_stat::TXN_COMMIT;
            }
            case T_Transaction_rollback: {
                run_res = "RollBack";
                return run_stat::TXN_ROLLBACK;
            }
            case T_Transaction_abort: {
                run_res = "Abort";
                return run_stat::TXN_ABORT;
            }
            default: {
                throw LJ::InternalError("Error");
                break;
            }
        }
    }
    throw LJ::InternalError("run_cmd_utility: unsupported plan type");
}
void QlManager::select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols , DTX *dtx){
    std::vector<std::string> captions;
    run_res.clear();

    Context context;
    context.m_data_send = new char[BUFFER_LENGTH];
    context.m_offset = new int(0);
    context.m_ellipsis = false;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }

    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(&context);
    rec_printer.print_record(captions , &context);
    rec_printer.print_separator(&context);

    size_t num_rec = 0;
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        DataItemPtr print_item = executorTreeRoot->Next();

        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = (char*)print_item->value + col.offset;
            if (col.type == ColType::TYPE_INT) {
                col_str = std::to_string(*(int*)rec_buf);
            }else if (col.type == ColType::TYPE_FLOAT) {
                col_str = std::to_string(*(float*)rec_buf);
            }else if (col.type == ColType::TYPE_STRING) {
                col_str = std::string((char *)rec_buf , col.len);
                col_str.resize(strlen(col_str.c_str()));
            }else if (col.type == ColType::TYPE_ITEMKEY){
                table_id_t tid = compute_server->get_node()->db_meta.get_table(col.tab_name).table_id;
                itemkey_t key = executorTreeRoot->getKey(tid);
                col_str = std::to_string(key);
            }
            columns.push_back(col_str);
        }

        rec_printer.print_record(columns, &context);
        ++num_rec;
    }

    if (dtx->tx_status != TXStatus::TX_ABORTING){
        rec_printer.print_separator(&context);
        RecordPrinter::print_record_count(num_rec , &context);

        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            run_res.assign(context.m_data_send, *context.m_offset);
        }
    }
    delete[] context.m_data_send;
    delete context.m_offset;
}

void QlManager::run_dml(std::shared_ptr<AbstractExecutor> exec){
    exec->Next();
    run_res = "affect raw : " + std::to_string(exec->getAffectRows());
}
