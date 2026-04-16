#pragma once

#include <string>
#include <vector>

namespace affinity {

// Auto-launch the parmetis_sidecar mpirun cluster from the leader compute_server
// (machine_id == 0). Idempotent and a no-op on non-leaders. Returns true when
// no spawn was needed or the spawn succeeded; false on hostfile/fork failure.
//
// Leader-only because mpirun itself SSH-launches sidecars on every node — we'd
// get N*N processes if every compute_server called it. PartitionerLoop's UDS
// reconnect handles the start-up race naturally across all ranks.
bool SpawnSidecarsIfLeader(const std::vector<std::string>& compute_ips,
                           int machine_id);

// SIGTERM the spawned mpirun process group and reap. Idempotent. Wired to
// atexit() and SIGTERM/SIGINT/SIGHUP from inside SpawnSidecarsIfLeader.
void StopSidecars();

}  // namespace affinity
