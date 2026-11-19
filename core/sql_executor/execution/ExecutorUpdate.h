#pragma once

#include "ExecutorAbstract.h"

class UpdateExecutor : public AbstractExecutor {
public:
    UpdateExecutor(DTX* dtx, const std::string& name, std::vector<SetClause> clauses,
                   std::unique_ptr<AbstractExecutor> scan)
        : dtx_(dtx), table_(dtx->compute_server->get_node()->db_meta.get_table(name)),
          clauses_(std::move(clauses)), scan_(std::move(scan)) {
        for (const auto& clause : clauses_)
            if (table_.is_primary(clause.lhs.col_name))
                throw std::logic_error("不允许修改主键");
    }

    DataItemPtr Next() override {
        m_affect_rows = 0;
        for (const auto& [rid, key] : collect_write_candidates(*scan_)) {
            dtx_->SqlWrite(table_.table_id, key, rid, false, [&](DataItem& row) {
                for (const auto& clause : clauses_) {
                    const auto col = table_.get_col(clause.lhs.col_name);
                    CHECK(col.offset >= 0 && col.offset + col.len <= row.value_size);
                    memcpy(row.value + col.offset, clause.rhs.data_item->value, col.len);
                }
            });
            ++m_affect_rows;
        }
        return nullptr;
    }

    Rid& rid() override { return m_abstractRid; }
    TabMeta getTab() const override { return table_; }

private:
    DTX* dtx_;
    TabMeta table_;
    std::vector<SetClause> clauses_;
    std::unique_ptr<AbstractExecutor> scan_;
};
