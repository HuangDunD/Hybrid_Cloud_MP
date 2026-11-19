#pragma once

#include "ExecutorAbstract.h"

#include "error_library.h"

class InsertExecutor : public AbstractExecutor {
public:
    InsertExecutor(DTX *dtx_ , const std::string &tab_name , std::vector<Value> values){
        dtx = dtx_;
        m_tableName = tab_name;
        m_values = values;
        if (!dtx->compute_server->table_exist(tab_name)){
            throw LJ::TableNotFoundError(tab_name);
        }

        m_tab = dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        if (m_tab.cols.size() != values.size()){
            throw LJ::ValuesCountMismatchError((int)m_tab.cols.size(), (int)values.size(), m_tableName);
        }

        m_rid = {.page_no_ = INVALID_PAGE_ID , .slot_no_ = -1};

        file_hdr = dtx->compute_server->get_file_hdr(m_tab.table_id);
    }

    // 对于 Insert 来说，Next() 就是直接执行插入了
    DataItemPtr Next() override {
        m_affect_rows = 0;
        int value_size = file_hdr->record_size_ - static_cast<int>(sizeof(DataItem));
        auto insert_item = std::make_shared<DataItem>(m_tab.table_id , value_size);
        itemkey_t primary_key = 0;
        bool has_pri = false;

        // 把每一列的数据顺序组织起来，构成一个完成的要插入的数据
        for (size_t i = 0 ; i < m_values.size() ; i++) {
            auto &val = m_values[i];
            auto &col = m_tab.cols[i];
            
            // 提取主键
            if (col.type == ColType::TYPE_ITEMKEY){
                if (val.type != TYPE_INT) throw std::logic_error("Expected integer primary key");
                primary_key = val.int_val;
                has_pri = true;
                continue;           // 主键不放在 DataItem 里，单独存的
            }
            if (col.type != val.type) throw std::logic_error("Invalid INSERT value type");
            
            val.init_dataItem(col.len);
            memcpy(insert_item->value + col.offset , val.data_item->value , col.len);
        }

        const auto table = m_tab.table_id;
        CHECK(has_pri == !m_tab.primary_key.empty());
        if (has_pri) m_rid = dtx->compute_server->get_rid_from_blink(table, primary_key);
        if (m_rid.page_no_ == INVALID_PAGE_ID)
            m_rid = dtx->ReserveSqlSlot(table, primary_key, has_pri, *file_hdr);

        dtx->SqlWrite(table, primary_key, m_rid, true, [&](DataItem& row) {
            CHECK(row.value_size == value_size);
            memcpy(row.value, insert_item->value, value_size);
            row.valid = 1;
        });
        m_affect_rows = 1;
        return nullptr;
    }

    Rid &rid() override {
        return m_rid;
    }

    TabMeta getTab() const override {
        return m_tab;
    }

private:
    DTX* dtx;

    TabMeta m_tab;
    std::vector<Value> m_values;
    std::string m_tableName;
    Rid m_rid{};

    RmFileHdr::ptr file_hdr;
};
