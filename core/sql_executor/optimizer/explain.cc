#include "plan.h"

#include <iomanip>
#include <limits>
#include <sstream>

namespace {
std::string column(const TabCol& col) {
    return col.tab_name.empty() ? col.col_name : col.tab_name + "." + col.col_name;
}

void print_value(std::ostream& out, const Value& value) {
    switch (value.type) {
        case TYPE_INT: out << value.int_val; break;
        case TYPE_ITEMKEY: out << value.item_val; break;
        case TYPE_FLOAT: out << std::setprecision(std::numeric_limits<float>::max_digits10) << value.float_val; break;
        case TYPE_STRING: out << std::quoted(value.str_val, '\''); break;
    }
}

void predicates(std::ostream& out, const char* label, const std::vector<Condition>& conds) {
    if (conds.empty()) return;
    out << label;
    for (size_t i = 0; i < conds.size(); ++i) {
        if (i) out << " AND ";
        const auto& c = conds[i];
        out << column(c.lhs_col) << CompOpString[c.op];
        if (c.is_rhs_val) print_value(out, c.rhs_val);
        else out << column(c.rhs_col);
    }
}

void print_node(std::ostream& out, const Plan& plan, unsigned depth) {
    const std::string indent(depth * 2, ' ');
    if (auto scan = dynamic_cast<const ScanPlan*>(&plan)) {
        if (scan->m_pointKey)
            out << indent << "PrimaryKeyLookup " << scan->m_tableName << " key=" << *scan->m_pointKey;
        else
            out << indent << "SeqScan " << scan->m_tableName;
        predicates(out, " filter=", scan->m_filterConds);
    } else if (auto join = dynamic_cast<const JoinPlan*>(&plan)) {
        out << indent << "NestedLoopJoin";
        predicates(out, " on=", join->m_conds);
        out << '\n';
        print_node(out, *join->m_left, depth + 1);
        print_node(out, *join->m_right, depth + 1);
        return;
    } else if (auto project = dynamic_cast<const ProjectionPlan*>(&plan)) {
        out << indent << "Project ";
        for (size_t i = 0; i < project->m_selCols.size(); ++i) {
            if (i) out << ", ";
            out << column(project->m_selCols[i]);
        }
        out << '\n';
        print_node(out, *project->m_subPlan, depth + 1);
        return;
    } else if (auto dml = dynamic_cast<const DMLPlan*>(&plan)) {
        switch (dml->m_tag) {
            case T_select:
                print_node(out, *dml->m_subPlan, depth);
                return;
            case T_Insert:
                out << indent << "Insert " << dml->m_tabName << " values=(";
                for (size_t i = 0; i < dml->m_values.size(); ++i) {
                    if (i) out << ", ";
                    print_value(out, dml->m_values[i]);
                }
                out << ')';
                break;
            case T_Update:
                out << indent << "Update " << dml->m_tabName << " set ";
                for (size_t i = 0; i < dml->m_setClauses.size(); ++i) {
                    if (i) out << ", ";
                    const auto& set = dml->m_setClauses[i];
                    out << column(set.lhs) << '=';
                    print_value(out, set.rhs);
                }
                break;
            case T_Delete: out << indent << "Delete " << dml->m_tabName; break;
            default: throw std::logic_error("Unsupported DML plan");
        }
        if (dml->m_subPlan) {
            out << '\n';
            print_node(out, *dml->m_subPlan, depth + 1);
            return;
        }
    } else {
        throw std::logic_error("Unsupported EXPLAIN plan");
    }
    out << '\n';
}
}  // namespace

std::string Explain(const Plan& plan) {
    std::ostringstream out;
    print_node(out, plan, 0);
    return out.str();
}
