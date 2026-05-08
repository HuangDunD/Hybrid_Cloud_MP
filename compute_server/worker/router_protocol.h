// Author: MingTai
// Router protocol for the interactive port (LOOKUP / SB commands).
// See compute_server/worker/router_protocol.cc for wire format.

#pragma once

#include <string>

class DTX;
class MetaManager;

namespace router_protocol {

// Returns true if `line` was recognized as a router-protocol command (LOOKUP/SB)
// and the response has been written to `sock`. Returns false otherwise; the
// caller should fall back to its existing parser.
//
// Only enabled when `bench_name` is "smallbank" or "smallbank_aff" — table_id
// semantics (0=savings, 1=checking) are SmallBank-specific.
bool TryHandleRouterCommand(int sock,
                            const std::string& line,
                            DTX* dtx,
                            MetaManager* meta_man,
                            const std::string& bench_name);

}  // namespace router_protocol
