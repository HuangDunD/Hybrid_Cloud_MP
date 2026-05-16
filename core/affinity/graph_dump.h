#pragma once

#include <string>

#include "graph.h"

namespace affinity {

bool DumpLocalGraphCsv(const LocalGraph& graph,
                       const std::string& path,
                       std::string* error = nullptr);

}  // namespace affinity
