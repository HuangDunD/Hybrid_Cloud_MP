#pragma once
#include <gflags/gflags.h>

namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

// brpc may default FATAL/CHECK to logging without terminating. Set this once
// during process startup, after flag parsing and before starting any workers.
inline void EnableFatalChecks() { logging::FLAGS_crash_on_fatal_log = true; }
