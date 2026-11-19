#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "sql_executor/sql_common.h"
#include "storage/sm_meta.h"
#include "compute_server/server.h"
#include "sql_executor/parser/ast.h"

class Plan{
public:
    PlanTag m_tag;
    int m_sqlID;
    int m_planID;

    Plan(int sql_id = -1, int plan_id = -1) : m_sqlID(sql_id), m_planID(plan_id) {}

    virtual ~Plan() = default;
    virtual int serialize(char* dest) = 0; 
    virtual int plan_tree_size() = 0; 
    virtual void format_print() = 0;
};

// Describe the physical DML tree before constructing or running executors.
std::string Explain(const Plan& plan);

class ScanPlan : public Plan {
public:
    ScanPlan(int sql_id, int plan_id, std::string table, std::optional<itemkey_t> key,
             std::vector<Condition> filters)
        : Plan(sql_id, plan_id), m_tableName(std::move(table)), m_pointKey(key),
          m_filterConds(std::move(filters)) {
        m_tag = key.has_value() ? T_BPTreeIndexScan : T_SeqScan;
    }

    void format_print() override { std::cout << Explain(*this); }

    int plan_tree_size() override {
        return 1;
    }

    int serialize(char* dest) {
        int offset = sizeof(int);
        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);

        // plantag, tot_size, table_id,
        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        int tab_name_size = m_tableName.length();
        memcpy(dest + offset, &tab_name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, m_tableName.c_str(), tab_name_size);
        offset += tab_name_size;

        // int col_num = cols_.size();
        // memcpy(dest + offset, &col_num, sizeof(int));
        // offset += col_num;
        // for(auto& col: cols_) col.serialize(dest, offset);
        int filter_cond_num = m_filterConds.size();
        memcpy(dest + offset, &filter_cond_num, sizeof(int));
        offset += sizeof(int);
        for (auto& filter_cond : m_filterConds) filter_cond.serialize(dest, offset);

        // Keep the legacy index-condition count, followed by the bound point key.
        int index_cond_num = 0;
        memcpy(dest + offset, &index_cond_num, sizeof(int));
        offset += sizeof(int);
        if (m_pointKey) {
            memcpy(dest + offset, &*m_pointKey, sizeof(itemkey_t));
            offset += sizeof(itemkey_t);
        }

        memcpy(dest, &offset, sizeof(int));
        return offset;
    }

    static std::shared_ptr<Plan> deserialize(char* src, ComputeServer *server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int tab_name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::string tab_name_(src + offset, tab_name_size);
        offset += tab_name_size;

        int filter_cond_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<Condition> filter_conds_;
        for (int i = 0; i < filter_cond_num; ++i) {
            Condition cond;
            cond.deserialize(src, offset);
            filter_conds_.push_back(std::move(cond));
        }

        // int len_ = *reinterpret_cast<const size_t*>(src + offset);
        // offset += sizeof(size_t);
        int index_cond_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<Condition> index_conds_;
        for (int i = 0; i < index_cond_num; ++i) {
            Condition cond;
            cond.deserialize(src, offset);
            index_conds_.push_back(std::move(cond));
        }

        std::optional<itemkey_t> key;
        if (tag == T_BPTreeIndexScan) {
            if (tot_size - offset == sizeof(itemkey_t)) {
                itemkey_t stored;
                memcpy(&stored, src + offset, sizeof(stored));
                key = stored;
            } else if (offset == tot_size) {
                // Older point plans kept the lookup predicate among their filters.
                const auto& primary = server->get_node()->db_meta.get_table(tab_name_).primary_key;
                for (const auto& c : filter_conds_)
                    if (c.is_rhs_val && c.op == OP_EQ && c.lhs_col.col_name == primary) {
                        key = c.rhs_val.int_val;
                        break;
                    }
            }
            if (!key) throw std::logic_error("Missing primary key in scan plan");
        } else if (tag != T_SeqScan) {
            throw std::logic_error("Unsupported scan plan");
        }
        return std::make_shared<ScanPlan>(sql_id, plan_id, std::move(tab_name_), key, std::move(filter_conds_));
    }

    std::string m_tableName;
    std::optional<itemkey_t> m_pointKey;
    std::vector<Condition> m_filterConds;
};


class JoinPlan : public Plan {
public:
    JoinPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right,
                 std::vector<Condition> conds)
        : Plan(sql_id, plan_id) {
        Plan::m_tag = tag;
        m_left = std::move(left);
        m_right = std::move(right);
        m_conds = std::move(conds);
        type = INNER_JOIN;

    }

    void format_print() override { std::cout << Explain(*this); }

    int plan_tree_size() override {
        return m_left->plan_tree_size() + m_right->plan_tree_size() + 1;
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);

        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        int cond_num = m_conds.size();
        memcpy(dest + offset, &cond_num, sizeof(int));
        offset += sizeof(int);
        for (auto& cond : m_conds) cond.serialize(dest, offset);

        int off_left = 0; // the offset of left_ (start from the end of the current operator)
        int off_right = 0; // the offset of righth_ (start from the end of the current operator)
        // | tot_size of this plan | cond_num | conds | off_left | off_right| ... left_ ... | right_ |
        memcpy(dest + offset, &off_left, sizeof(int));
        offset += sizeof(int);

        off_right = m_left->serialize(dest + offset + sizeof(int));

        memcpy(dest + offset, &off_right, sizeof(int));
        offset += sizeof(int);

        memcpy(dest, &offset, sizeof(int));

        int right_size = m_right->serialize(dest + off_right + offset);

        return right_size + offset + off_right;
    }

    static std::shared_ptr<JoinPlan> deserialize(char* src, ComputeServer* server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int cond_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<Condition> conds_;
        for (int i = 0; i < cond_num; ++i) {
            Condition cond;
            cond.deserialize(src, offset);
            conds_.push_back(std::move(cond));
        }

        int off_left = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int off_right = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        assert(tot_size == offset);
        src = src + offset;
        PlanTag left_tag = *reinterpret_cast<const PlanTag*>(src + off_left + sizeof(int));
        PlanTag right_tag = *reinterpret_cast<const PlanTag*>(src + off_right + sizeof(int));

        std::shared_ptr<Plan> left_;
        std::shared_ptr<Plan> right_;

        if (left_tag == PlanTag::T_NestLoop) {
            left_ = JoinPlan::deserialize(src + off_left, server);
        }
        else if (left_tag == PlanTag::T_SeqScan || left_tag == PlanTag::T_BPTreeIndexScan || left_tag == PlanTag::T_HashIndexScan) {
            left_ = ScanPlan::deserialize(src + off_left, server);
        }

        if (right_tag == PlanTag::T_NestLoop) {
            right_ = JoinPlan::deserialize(src + off_right, server);
        }
        else if (right_tag == PlanTag::T_SeqScan || right_tag == PlanTag::T_BPTreeIndexScan || right_tag == PlanTag::T_HashIndexScan) {
            right_ = ScanPlan::deserialize(src + off_right, server);
        }

        return std::make_shared<JoinPlan>(tag, sql_id, plan_id, left_, right_, conds_);
    }

    // 左节点
    std::shared_ptr<Plan> m_left;
    // 右节点
    std::shared_ptr<Plan> m_right;
    // 连接条件
    std::vector<Condition> m_conds;
    // future TODO: 后续可以支持的连接类型
    JoinType type;
};

class SortPlan : public Plan {
public:
    SortPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc)
        : Plan(sql_id, plan_id) {
        Plan::m_tag = tag;
        subplan_ = std::move(subplan);
        sel_col_ = sel_col;
        is_desc_ = is_desc;
    }

    ~SortPlan() {}

    void format_print() override {}

    int plan_tree_size() override {
        return 1 + subplan_->plan_tree_size();
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);

        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        sel_col_.serialize(dest, offset);

        memcpy(dest + offset, &is_desc_, sizeof(bool));
        offset += sizeof(bool);

        int off_subplan = 0;
        memcpy(dest + offset, &off_subplan, sizeof(int));
        offset += sizeof(int);

        memcpy(dest, &offset, sizeof(int));

        int subplan_size = subplan_->serialize(dest + offset);
        return offset + subplan_size;
    }

    static std::shared_ptr<SortPlan> deserialize(char* src, ComputeServer* server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        TabCol sel_col_;
        sel_col_.deserialize(src, offset);

        bool is_desc_ = *reinterpret_cast<const bool*>(src + offset);
        offset += sizeof(bool);

        int off_subplan = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        assert(offset == tot_size);
        src = src + offset;
        PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
        std::shared_ptr<Plan> subplan_;

        if (subplan_tag == PlanTag::T_NestLoop) {
            subplan_ = JoinPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_SeqScan || subplan_tag == PlanTag::T_BPTreeIndexScan || subplan_tag == PlanTag::T_HashIndexScan) {
            subplan_ = ScanPlan::deserialize(src + off_subplan, server);
        }

        return std::make_shared<SortPlan>(tag, sql_id, plan_id, subplan_, sel_col_, is_desc_);
    }

    std::shared_ptr<Plan> subplan_;
    TabCol sel_col_;
    bool is_desc_;
};

class ProjectionPlan : public Plan{
public:
    ProjectionPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> subplan,
                    std::vector<TabCol> sel_cols)
        : Plan(sql_id, plan_id) {
        Plan::m_tag = tag;
        m_subPlan = std::move(subplan);
        m_selCols = std::move(sel_cols);
    }

    ~ProjectionPlan() {}

    void format_print() override { std::cout << Explain(*this); }

    int plan_tree_size() override {
        return 1 + m_subPlan->plan_tree_size();
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);

        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        int col_num = m_selCols.size();
        memcpy(dest + offset, &col_num, sizeof(int));
        offset += sizeof(int);
        for (auto& col : m_selCols) col.serialize(dest, offset);

        int off_subplan = 0;
        memcpy(dest + offset, &off_subplan, sizeof(int));
        offset += sizeof(int);

        memcpy(dest, &offset, sizeof(int));

        int subplan_size = m_subPlan->serialize(dest + offset);

        return offset + subplan_size;
    }

    static std::shared_ptr<ProjectionPlan> deserialize(char* src, ComputeServer* server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int col_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<TabCol> sel_cols_;
        for (int i = 0; i < col_num; ++i) {
            TabCol col;
            col.deserialize(src, offset);
            sel_cols_.push_back(std::move(col));
        }

        int off_subplan = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        assert(offset == tot_size);
        src = src + offset;
        PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
        std::shared_ptr<Plan> subplan_;

        if (subplan_tag == PlanTag::T_NestLoop) {
            subplan_ = JoinPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_SeqScan || subplan_tag == PlanTag::T_BPTreeIndexScan || subplan_tag == PlanTag::T_HashIndexScan ) {
            subplan_ = ScanPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_Sort) {
            subplan_ = SortPlan::deserialize(src + off_subplan, server);
        }

        return std::make_shared<ProjectionPlan>(tag, sql_id, plan_id, subplan_, sel_cols_);
    }

    std::shared_ptr<Plan> m_subPlan;
    std::vector<TabCol> m_selCols;
};

class GatherPlan : public Plan{
public:
    GatherPlan(PlanTag tag, int sql_id, int plan_id, std::vector<std::shared_ptr<Plan>>& subplans) : Plan(
        sql_id, plan_id) {
        Plan::m_tag = tag;
        m_subPlans = std::move(subplans);
    }

    int serialize(char* dest) override {
        return -1;
    }

    int plan_tree_size() override {
        return 1;
    }

    void format_print() override {
        std::cout << "op_id: " << m_planID << ", ";
        std::cout << "Gather: ";
        for (auto& plan : m_subPlans) {
            plan->format_print();
        }
    }

    std::vector<std::shared_ptr<Plan>> m_subPlans;
};

class DMLPlan : public Plan{
public:
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, int tab_id,
            std::vector<Value> values, std::vector<Condition> conds,
            std::vector<SetClause> set_clauses) {
        Plan::m_tag = tag;
        m_subPlan = std::move(subplan);
        m_tabName = std::move(tab_name);
        m_tabID = tab_id;
        m_values = std::move(values);
        m_conds = std::move(conds);
        m_setClauses = std::move(set_clauses);
    }

    void format_print() override { std::cout << Explain(*this); }

    int plan_tree_size() override {
        return 1 + m_subPlan->plan_tree_size();
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);


        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        int tab_name_size = m_tabName.length();
        memcpy(dest + offset, &tab_name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, m_tabName.c_str(), tab_name_size);
        offset += tab_name_size;

        memcpy(dest + offset, &m_tabID, sizeof(int));
        offset += sizeof(int);

        int val_num = m_values.size();
        memcpy(dest + offset, &val_num, sizeof(int));
        offset += sizeof(int);
        for (auto& val : m_values) val.serialize(dest, offset);

        int cond_num = m_conds.size();
        memcpy(dest + offset, &cond_num, sizeof(int));
        offset += sizeof(int);
        for (auto& cond : m_conds) cond.serialize(dest, offset);

        int clause_num = m_setClauses.size();
        memcpy(dest + offset, &clause_num, sizeof(int));
        offset += sizeof(int);
        for (auto& clause : m_setClauses) clause.serialize(dest, offset);

        int off_subplan = 0;
        memcpy(dest + offset, &off_subplan, sizeof(int));
        offset += sizeof(int);

        memcpy(dest, &offset, sizeof(int));

        int subplan_size = m_subPlan->serialize(dest + offset);

        return offset + subplan_size;
    }

    static std::shared_ptr<DMLPlan> deserialize(char* src, ComputeServer* server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);


        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int tab_name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::string tab_name_(src + offset, tab_name_size);
        offset += tab_name_size;

        int tab_id_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        int val_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<Value> values_;
        for (int i = 0; i < val_num; ++i) {
            Value val;
            val.deserialize(src, offset);
            values_.push_back(std::move(val));
        }

        int cond_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<Condition> conds_;
        for (int i = 0; i < cond_num; ++i) {
            Condition cond;
            cond.deserialize(src, offset);
            conds_.push_back(std::move(cond));
        }

        int clause_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<SetClause> set_clauses_;
        for (int i = 0; i < clause_num; ++i) {
            SetClause clause;
            clause.deserialize(src, offset);
            set_clauses_.push_back(std::move(clause));
        }

        int off_subplan = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        assert(offset == tot_size);
        src = src + offset;
        PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
        std::shared_ptr<Plan> subplan_;

        if (subplan_tag == PlanTag::T_Projection) {
            subplan_ = ProjectionPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_SeqScan || subplan_tag == PlanTag::T_BPTreeIndexScan || subplan_tag == PlanTag::T_HashIndexScan) {
            subplan_ = ScanPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_Sort) {
            subplan_ = SortPlan::deserialize(src + off_subplan, server);
        }
        else if (subplan_tag == PlanTag::T_NestLoop) {
            subplan_ = JoinPlan::deserialize(src + offset, server);
        }

        return std::make_shared<DMLPlan>(tag, subplan_, tab_name_, tab_id_, values_, conds_, set_clauses_);
    }

    ~DMLPlan() {}
    std::shared_ptr<Plan> m_subPlan;
    std::string m_tabName;
    int m_tabID;
    std::vector<Value> m_values;
    std::vector<Condition> m_conds;
    std::vector<SetClause> m_setClauses;
};

class DDLPlan : public Plan{
public:
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols,
            std::string pkey, IndexType index_type = IndexType::BTREE_INDEX) {
        Plan::m_tag = tag;
        m_tabName = std::move(tab_name);
        m_cols = std::move(cols);
        m_tabColNames = std::move(col_names);
        m_pkey = std::move(pkey);
        m_indexType = index_type;
    }

    ~DDLPlan() {}

    void format_print() override {}

    int plan_tree_size() override {
        return 1;
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);

        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);

        int tab_name_len = m_tabName.length();
        memcpy(dest + offset, &tab_name_len, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, m_tabName.c_str(), tab_name_len);
        offset += tab_name_len;

        int tab_col_size = m_tabColNames.size();
        memcpy(dest + offset, &tab_col_size, sizeof(int));
        offset += sizeof(int);
        for (auto& col_name : m_tabColNames) {
            int name_size = col_name.length();
            memcpy(dest + offset, &name_size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, col_name.c_str(), name_size);
            offset += name_size;
        }

        int col_num = m_cols.size();
        memcpy(dest + offset, &col_num, sizeof(int));
        offset += sizeof(int);
        for (auto& col : m_cols) col.serialize(dest, offset);

        int key_size = m_pkey.size();
        memcpy(dest + offset , &key_size , sizeof(int));
        offset += key_size;
        memcpy(dest + offset , m_pkey.c_str() , m_pkey.size());
        offset += m_pkey.size();

        memcpy(dest, &offset, sizeof(int));

        return offset;
    }

    static std::shared_ptr<DDLPlan> deserialize(char* src, ComputeServer* server) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);


        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int tab_name_len = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::string tab_name(src + offset, tab_name_len);
        offset += tab_name_len;

        int tab_col_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<std::string> tab_col_names_;
        for (int i = 0; i < tab_col_size; ++i) {
            int name_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::string col_name(src + offset, name_size);
            offset += name_size;
            tab_col_names_.push_back(std::move(col_name));
        }

        int col_num = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::vector<ColDef> cols_;
        for (int i = 0; i < col_num; ++i) {
            ColDef col;
            col.deserialize(src, offset);
            cols_.push_back(std::move(col));
        }

        int key_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::string pkey(src + offset , key_size);
        offset += key_size;

        assert(offset == tot_size);

        return std::make_shared<DDLPlan>(tag, tab_name, tab_col_names_, cols_, pkey);
    }

    std::string m_tabName;
    std::vector<std::string> m_tabColNames;
    std::vector<ColDef> m_cols;
    std::string m_pkey;
    IndexType m_indexType;
};

class OtherPlan : public Plan{
public:
    OtherPlan(PlanTag tag, std::string tab_name) {
        Plan::m_tag = tag;
        m_tabName = std::move(tab_name);
    }

    ~OtherPlan() {}

    void format_print() override {}

    int plan_tree_size() override {
        return 1;
    }

    int serialize(char* dest) override {
        int offset = sizeof(int);

        /*
            sql_id & plan_id
        */
        memcpy(dest + offset, (char*)&m_sqlID, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char*)&m_planID, sizeof(int));
        offset += sizeof(int);

        memcpy(dest + offset, &m_tag, sizeof(PlanTag));
        offset += sizeof(PlanTag);
        int tab_name_size = m_tabName.length();
        memcpy(dest + offset, &tab_name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, m_tabName.c_str(), tab_name_size);
        offset += tab_name_size;
        memcpy(dest, &offset, sizeof(int));
        return offset;
    }

    static std::shared_ptr<OtherPlan> deserialize(char* src, SmManager* sm_mgr) {
        int offset = 0;
        int tot_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        /*
            sql_id & plan_id
        */
        int sql_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        int plan_id = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);


        PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
        offset += sizeof(PlanTag);

        int tab_name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::string tab_name_(src + offset, tab_name_size);
        offset += tab_name_size;

        assert(offset == tot_size);

        return std::make_shared<OtherPlan>(tag, tab_name_);
    }

    std::string m_tabName;
};

class plannerInfo{
public:
    std::shared_ptr<ast::SelectStmt> m_parse;
    std::vector<Condition> m_whereConds;
    std::vector<TabCol> m_selCols;
    std::shared_ptr<Plan> m_plan;
    std::vector<std::shared_ptr<Plan>> m_tableScanExecutor;
    std::vector<SetClause> m_setClause;
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : m_parse(std::move(parse_)) {}
};