#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "plan.h"
#include "../analyze/analyze.h"

#include "compute_server/server.h"

class Planner {
public:
    typedef std::shared_ptr<Planner> ptr;
    Planner(ComputeServer *server) : compute_server(server) {}

    inline void set_sql_id(int sql_id) {
        current_sql_id_     = sql_id;
        current_plan_id_    = 0;
    }

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query);

private:
    std::shared_ptr<ScanPlan> build_scan(const std::string& table, std::vector<Condition> filters);
    std::shared_ptr<Plan> make_one_rel(const Query& query);

    ColType interp_sv_type(ast::SvType sv_type) {
        std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_STRING}};
        return m.at(sv_type);
    }

private:
    ComputeServer *compute_server;
    std::atomic<int>     current_sql_id_{0};
    std::atomic<int>     current_plan_id_{0};
};