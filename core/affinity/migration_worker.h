// Phase 5 — MigrationWorker. The background body of the affinity migration
// loop. Each tick:
//   1. Sweep AssignmentTable for tuples whose target != self -> Enqueue plans.
//   2. Drain up to `affinity_migration_batch` plans and execute MigrateOne()
//      for each.
//
// MigrateOne (Strategy A): copies the tuple bytes from the local source page
// to a freshly-allocated destination page on the target node, re-points the
// BLink index, and removes the source slot. WAL records guarantee log-replay
// consistency for whichever side comes back first after a crash. Note: BLink
// itself is not WAL-recoverable (see plan §1 risks); this is acknowledged as
// paper-level simplification — do not kill -9 mid-experiment.
#pragma once

#include <cstdint>

class ComputeServer;

namespace affinity {

void MigrationLoop(ComputeServer* cs);

void RequestMigrationStop();

// One-shot migration of a single tuple. Returns true if the tuple was
// successfully relocated, false on race (lost ownership) or transient error.
// Exposed for unit tests.
bool MigrateOne(ComputeServer* cs, uint64_t tuple_id, int dst_node);

}  // namespace affinity
