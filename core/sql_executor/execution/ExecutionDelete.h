#pragma once

#include "ExecutorAbstract.h"

class DeleteExecutor : public AbstractExecutor {
public:
    DeleteExecutor(DTX* dtx, const std::string& name,
                   std::unique_ptr<AbstractExecutor> scan)
        : dtx_(dtx), table_(dtx->compute_server->get_node()->db_meta.get_table(name)),
          scan_(std::move(scan)) {}

    DataItemPtr Next() override {
        m_affect_rows = 0;
        for (const auto& [rid, key] : collect_write_candidates(*scan_)) {
            dtx_->SqlWrite(table_.table_id, key, rid, false, [](DataItem& row) { row.valid = 0; });
            ++m_affect_rows;
        }
        return nullptr;
    }

    Rid& rid() override { return m_abstractRid; }
    TabMeta getTab() const override { return table_; }

private:
    DTX* dtx_;
    TabMeta table_;
    std::unique_ptr<AbstractExecutor> scan_;
};
