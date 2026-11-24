#pragma once

// Configure bRPC before creating any RPC channels or servers.
void ConfigureRpcTransport(bool enabled);

// Identifies the one timestamp protocol supported by this cluster at its startup barrier.
inline constexpr int kClusterStartTag = 0x54534f00;
