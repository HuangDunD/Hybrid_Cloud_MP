#include "planner.h"
#include "sql_executor/parser/ast.h"
#include <cassert>

std::shared_ptr<ScanPlan> Planner::build_scan(const std::string& table, std::vector<Condition> filters) {
    const auto& primary = compute_server->get_node()->db_meta.get_table(table).primary_key;
    std::optional<itemkey_t> key;
    for (auto it = filters.begin(); it != filters.end(); ++it) {
        if (it->op == OP_EQ && it->is_rhs_val && it->lhs_col.col_name == primary) {
            key = static_cast<itemkey_t>(it->rhs_val.int_val);
            filters.erase(it);  // Keep all other predicates, including a second equality on the key.
            break;
        }
    }
    return std::make_shared<ScanPlan>(current_sql_id_, current_plan_id_++, table, key, std::move(filters));
}

std::shared_ptr<Plan> Planner::make_one_rel(const Query& query) {
    const auto& tables = query.m_tables;
    if (tables.size() == 1) return build_scan(tables.front(), query.m_conds);
    if (tables.size() != 2) throw std::logic_error("Not Support 3 or more Table Execute");
    if (tables[0] == tables[1]) throw std::logic_error("Self joins require table aliases");

    std::vector<Condition> left_filters, right_filters, join_conditions;
    for (auto cond : query.m_conds) {
        if (cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name) {
            auto& filters = cond.lhs_col.tab_name == tables[0] ? left_filters : right_filters;
            filters.push_back(std::move(cond));
        } else {
            // JoinExecutor expects one operand from each child, in FROM order.
            if (cond.lhs_col.tab_name == tables[1]) {
                std::swap(cond.lhs_col, cond.rhs_col);
                static const std::map<CompOp, CompOp> reverse = {
                    {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT},
                    {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE}};
                cond.op = reverse.at(cond.op);
            }
            join_conditions.push_back(std::move(cond));
        }
    }
    auto left = build_scan(tables[0], std::move(left_filters));
    auto right = build_scan(tables[1], std::move(right_filters));
    return std::make_shared<JoinPlan>(T_NestLoop, current_sql_id_, current_plan_id_++,
                                      std::move(left), std::move(right), std::move(join_conditions));
}

std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query){
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->m_parse)){      // CreatTable
        std::vector<ColDef> col_defs;
        std::string pri_key = "";

        for (auto &field : x->fields) {
            // 尝试将字段转换为 ast::ColDef
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                // 创建内部使用的列定义结构体，包含：
                // - name: 列名（从AST中的col_name获取）
                // - type: 列类型（通过interp_sv_type函数将AST类型转换为内部类型）
                // - len: 列长度（对于CHAR类型有意义，INT/FLOAT类型忽略此值）
                ColDef col_def;
                col_def.name = sv_col_def->col_name;
                col_def.type = interp_sv_type(sv_col_def->type_len->type);
                col_def.len = sv_col_def->type_len->len;
                col_defs.push_back(col_def);
                
                // 检查是否为主键列
                if (sv_col_def->is_primary) {
                    // 应该只有一个主键列
                    if (pri_key != ""){
                        throw std::logic_error("主键只能是单个列！且需要为int类型");
                    }
                    pri_key = (sv_col_def->col_name);
                }
            }else {
                // 不应该走到这，保险点放个 assert
                assert(false);
                throw LJ::UnixError();
            }
        }

        // if (pri_key == ""){
        //     throw std::logic_error("主键不能为空，且需要为 INT 类型\n示例：Create Table student(id int , age int , primary key(id));");
        // }

        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable , x->tab_name ,std::vector<std::string>() , col_defs , pri_key);

    }else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->m_parse)){      // DropTable
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable , x->tab_name , std::vector<std::string>() , std::vector<ColDef>() , "");
    }else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->m_parse)){      // DropIndex
        // TODO 目前不支持 DropIndex
        throw LJ::UnsupportedOperationError("DropIndex" , "" , "");
    }else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->m_parse)){   // CreateIndex
        // TODO 目前不支持创建索引，唯一的索引是创建表的时候创建的 B+ 树索引
        throw LJ::UnsupportedOperationError("CreateIndex");
    }else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->m_parse)){     // Insert
        // TODO 目前插入的 table_id 设置为 0，后边看下怎么搞，其实只要拿到 tab_name 就可以通过 db_meta 解析出 table_id，先不管了
        plannerRoot = std::make_shared<DMLPlan>(T_Insert , std::shared_ptr<Plan>() ,
            x->m_tabName , 0 , query->values ,std::vector<Condition>() , std::vector<SetClause>());
    }else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->m_parse)){     // Update
        auto scan = build_scan(x->m_tabName, query->m_conds);
        plannerRoot = std::make_shared<DMLPlan>(T_Update, scan, x->m_tabName, -1, std::vector<Value>{},
                                                query->m_conds, query->m_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->m_parse)) {
        auto scan = build_scan(x->m_tabName, query->m_conds);
        plannerRoot = std::make_shared<DMLPlan>(T_Delete, scan, x->m_tabName, -1, std::vector<Value>{},
                                                query->m_conds, std::vector<SetClause>{});
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->m_parse)) {
        if (x->has_sort) throw std::logic_error("ORDER BY is not supported");
        auto input = make_one_rel(*query);
        auto projection = std::make_shared<ProjectionPlan>(T_Projection, current_sql_id_, current_plan_id_++,
                                                            std::move(input), query->m_cols);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), -1 , std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    }else {
        throw LJ::UnixError();
    }
    return plannerRoot;
}
