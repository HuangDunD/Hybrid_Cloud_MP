// Author: MingTai
//
// Router protocol implementation. Two ASCII commands, each terminated by '\n',
// optionally trailed by '\0' (which we tolerate). Responses are NUL-terminated
// to match the existing `send_response` helper used by the SQL handler.
//
//   LOOKUP <table_id> <start_key> <count>
//     -> "OK <count>\n<key0> <page0> <slot0>\n..."  (page=-1 if not found)
//
//   SB <txn_id> <txn_type> <a1> [<a2>...]
//     -> "OK <accessed_count>\n<table_id0> <key0> <page0> <owner0>\n..."
//     -> "ABORT <txn_id> <reason>\n"
//
// txn_type follows MP-Router's serve/test/smallbank.h ordering:
//   0=Amalgamate(a1,a2), 1=SendPayment(a1,a2), 2=DepositChecking(a1),
//   3=WriteCheck(a1), 4=Balance(a1), 5=TransactSavings(a1), 6=MultiUpdate(a1..)
// Account IDs use the SmallBank loader's 0-based namespace
// (PopulateSavingsTable inserts keys 0..num_accounts_global-1).

#include "router_protocol.h"

#include <sys/socket.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "affinity/affinity_config.h"  // pulls in extern bool enable_affinity
#include "connection/meta_manager.h"
#include "dtx/dtx.h"
#include "smallbank/smallbank_db.h"

namespace router_protocol {

namespace {

bool SendAll(int sock, const char* data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = send(sock, data + sent, len - sent, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (n == 0) {
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

inline void SendNul(int sock, const std::string& s) {
  // Mirror compute_server/worker/worker.cc:42-44 send_response: include the
  // trailing NUL so MP-Router can frame on '\0'.
  (void)SendAll(sock, s.c_str(), s.size() + 1);
}

inline std::string RTrim(const std::string& s) {
  size_t r = s.size();
  while (r > 0) {
    char c = s[r - 1];
    if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\0') {
      --r;
    } else {
      break;
    }
  }
  return s.substr(0, r);
}

// Tokenize on runs of whitespace.
std::vector<std::string> Split(const std::string& s) {
  std::vector<std::string> out;
  size_t i = 0;
  const size_t n = s.size();
  while (i < n) {
    while (i < n && (s[i] == ' ' || s[i] == '\t')) ++i;
    if (i == n) break;
    size_t j = i;
    while (j < n && s[j] != ' ' && s[j] != '\t') ++j;
    out.emplace_back(s.substr(i, j - i));
    i = j;
  }
  return out;
}

bool ParseUll(const std::string& s, uint64_t& out) {
  if (s.empty()) return false;
  try {
    size_t pos = 0;
    out = std::stoull(s, &pos);
    return pos == s.size();
  } catch (...) {
    return false;
  }
}

bool ParseInt(const std::string& s, int& out) {
  if (s.empty()) return false;
  try {
    size_t pos = 0;
    out = std::stoi(s, &pos);
    return pos == s.size();
  } catch (...) {
    return false;
  }
}

// Case-insensitive prefix check on the first whitespace-delimited token.
bool FirstTokenIs(const std::string& s, const char* expect) {
  size_t i = 0;
  while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
  size_t j = i;
  while (j < s.size() && s[j] != ' ' && s[j] != '\t') ++j;
  if (j - i != std::strlen(expect)) return false;
  for (size_t k = 0; k < j - i; ++k) {
    char c = s[i + k];
    if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a');
    char e = expect[k];
    if (e >= 'A' && e <= 'Z') e = char(e - 'A' + 'a');
    if (c != e) return false;
  }
  return true;
}

bool IsSmallBankBenchName(const std::string& bn) {
  return bn == "smallbank" || bn == "smallbank_aff";
}

// =============================================================================
// LOOKUP
// =============================================================================
void HandleLookup(int sock, const std::vector<std::string>& tok,
                  MetaManager* meta_man) {
  // tok[0]="LOOKUP" tok[1]=table_id tok[2]=start_key tok[3]=count
  if (tok.size() != 4) {
    SendNul(sock, "ERR LOOKUP usage: LOOKUP <table_id> <start_key> <count>");
    return;
  }
  int table_id = 0;
  uint64_t start_key = 0;
  int count = 0;
  if (!ParseInt(tok[1], table_id) || !ParseUll(tok[2], start_key) ||
      !ParseInt(tok[3], count) || count < 0) {
    SendNul(sock, "ERR LOOKUP bad args");
    return;
  }
  std::ostringstream oss;
  oss << "OK " << count << "\n";
  for (int i = 0; i < count; ++i) {
    uint64_t key = start_key + (uint64_t)i;
    Rid rid = meta_man->Fetchrid(table_id, (itemkey_t)key);
    // Index cache returns page_no_=-1 / slot_no_=-1 (or large sentinel) when the
    // key is missing. Coerce to -1 -1 so MP-Router can treat it uniformly.
    int64_t page = (int64_t)rid.page_no_;
    int slot = rid.slot_no_;
    if (page <= 0 || page > (int64_t)0x7fffffff) {
      page = -1;
      slot = -1;
    }
    oss << key << " " << page << " " << slot << "\n";
  }
  SendNul(sock, oss.str());
}

// =============================================================================
// SB — bare DTX template. Follows MP-Router serve/test/smallbank.h ordering for
// (table, key, rw) so that update_key_page sees touched entries in the order it
// expects from TABLE_IDS_ARR[txn_type].
// =============================================================================

constexpr int kSavingsTable = 0;   // SmallBankTableType::kSavingsTable
constexpr int kCheckingTable = 1;  // SmallBankTableType::kCheckingTable

struct AccessSpec {
  int table_id;
  uint64_t key;
  bool is_write;
};

// Returns false if txn_type / accounts do not match the expected shape.
bool BuildAccesses(int txn_type, const std::vector<uint64_t>& accts,
                   std::vector<AccessSpec>& out) {
  out.clear();
  switch (txn_type) {
    case 0:  // Amalgamate(a1, a2): {checking,a1,RW}, {savings,a1,RW}, {checking,a2,RW}
      if (accts.size() != 2) return false;
      out.push_back({kCheckingTable, accts[0], true});
      out.push_back({kSavingsTable, accts[0], true});
      out.push_back({kCheckingTable, accts[1], true});
      return true;
    case 1:  // SendPayment(a1, a2): {checking,a1,RW}, {checking,a2,RW}
      if (accts.size() != 2) return false;
      out.push_back({kCheckingTable, accts[0], true});
      out.push_back({kCheckingTable, accts[1], true});
      return true;
    case 2:  // DepositChecking(a1): {checking,a1,RW}
      if (accts.size() != 1) return false;
      out.push_back({kCheckingTable, accts[0], true});
      return true;
    case 3:  // WriteCheck(a1): {savings,a1,RO}, {checking,a1,RW}
      if (accts.size() != 1) return false;
      out.push_back({kSavingsTable, accts[0], false});
      out.push_back({kCheckingTable, accts[0], true});
      return true;
    case 4:  // Balance(a1): {checking,a1,RO}, {savings,a1,RO}
      if (accts.size() != 1) return false;
      out.push_back({kCheckingTable, accts[0], false});
      out.push_back({kSavingsTable, accts[0], false});
      return true;
    case 5:  // TransactSavings(a1): {savings,a1,RW}
      if (accts.size() != 1) return false;
      out.push_back({kSavingsTable, accts[0], true});
      return true;
    case 6:  // MultiUpdate(a1, a2, ...): {checking,ai,RW} for each
      if (accts.empty()) return false;
      for (uint64_t k : accts) {
        out.push_back({kCheckingTable, k, true});
      }
      return true;
    default:
      return false;
  }
}

inline int ValueSizeForTable(int table_id) {
  if (table_id == kSavingsTable) return (int)sizeof(smallbank_savings_val_t);
  return (int)sizeof(smallbank_checking_val_t);
}

void HandleSB(int sock, const std::vector<std::string>& tok, DTX* dtx,
              MetaManager* meta_man) {
  // tok[0]="SB" tok[1]=txn_id tok[2]=txn_type tok[3..]=accounts
  if (tok.size() < 4) {
    SendNul(sock, "ERR SB usage: SB <txn_id> <txn_type> <a1> [<a2>...]");
    return;
  }
  uint64_t txn_id = 0;
  int txn_type = 0;
  if (!ParseUll(tok[1], txn_id) || !ParseInt(tok[2], txn_type)) {
    SendNul(sock, "ERR SB bad header");
    return;
  }
  std::vector<uint64_t> accts;
  accts.reserve(tok.size() - 3);
  for (size_t i = 3; i < tok.size(); ++i) {
    uint64_t v = 0;
    if (!ParseUll(tok[i], v)) {
      SendNul(sock, "ERR SB bad account at idx " + std::to_string(i - 3));
      return;
    }
    accts.push_back(v);
  }
  std::vector<AccessSpec> accesses;
  if (!BuildAccesses(txn_type, accts, accesses)) {
    SendNul(sock, "ABORT " + std::to_string(txn_id) +
                      " bad txn_type=" + std::to_string(txn_type) +
                      " or arity");
    return;
  }

  coro_yield_t fake_yield;

  try {
    dtx->TxBegin((tx_id_t)txn_id);
    dtx->DecideCommitMode();

    // Parallel vector tracking the index of each access in its set, so we can
    // recover the (table, key, value*) tuple post-execution.
    struct AccessRef {
      int set;       // 0 = read_only_set, 1 = read_write_set
      size_t idx;    // index within that set
    };
    std::vector<AccessRef> refs;
    refs.reserve(accesses.size());

    for (const auto& a : accesses) {
      auto item = std::make_shared<DataItem>((table_id_t)a.table_id,
                                              ValueSizeForTable(a.table_id));
      if (a.is_write) {
        dtx->AddToReadWriteSet(item, (itemkey_t)a.key);
        refs.push_back({1, dtx->read_write_set.size() - 1});
      } else {
        dtx->AddToReadOnlySet(item, (itemkey_t)a.key);
        refs.push_back({0, dtx->read_only_set.size() - 1});
      }
    }

    bool exe_ok = dtx->TxExe(fake_yield, /*fail_abort=*/false);
    if (!exe_ok) {
      dtx->TxAbortWorkLoad(fake_yield);
      SendNul(sock, "ABORT " + std::to_string(txn_id) + " exe_failed");
      return;
    }

    // Touch values: verify magic for everything, and for write entries do a
    // benign in-place mutation so the DTX produces a real undo/redo footprint.
    for (size_t i = 0; i < accesses.size(); ++i) {
      const auto& a = accesses[i];
      DataSetItem* dsi = (refs[i].set == 1)
                             ? &dtx->read_write_set[refs[i].idx].second
                             : &dtx->read_only_set[refs[i].idx].second;
      if (dsi == nullptr || !dsi->is_fetched || dsi->item_ptr == nullptr ||
          dsi->item_ptr->value == nullptr) {
        dtx->TxAbortWorkLoad(fake_yield);
        SendNul(sock, "ABORT " + std::to_string(txn_id) +
                          " fetch_missing table=" + std::to_string(a.table_id) +
                          " key=" + std::to_string(a.key));
        return;
      }
      if (a.table_id == kSavingsTable) {
        auto* v = reinterpret_cast<smallbank_savings_val_t*>(dsi->item_ptr->value);
        if (v->magic != smallbank_savings_magic) {
          dtx->TxAbortWorkLoad(fake_yield);
          SendNul(sock, "ABORT " + std::to_string(txn_id) +
                            " magic_mismatch table=0 key=" +
                            std::to_string(a.key));
          return;
        }
        if (a.is_write) v->bal += 1.0f;
      } else {
        auto* v = reinterpret_cast<smallbank_checking_val_t*>(dsi->item_ptr->value);
        if (v->magic != smallbank_checking_magic) {
          dtx->TxAbortWorkLoad(fake_yield);
          SendNul(sock, "ABORT " + std::to_string(txn_id) +
                            " magic_mismatch table=1 key=" +
                            std::to_string(a.key));
          return;
        }
        if (a.is_write) v->bal += 1.0f;
      }
    }

    bool commit_ok = dtx->TxCommit(fake_yield);
    if (!commit_ok) {
      SendNul(sock, "ABORT " + std::to_string(txn_id) + " commit_failed");
      return;
    }

    // Post-commit lookup of pages so the response reflects affinity migration.
    //
    // When affinity is on, the static IndexCache that backs MetaManager::Fetchrid
    // is the prefetched startup snapshot — migration_worker only updates BLink
    // (see core/affinity/migration_worker.cc:385 update_blink_entry), so
    // Fetchrid would still return the old page and MP-Router's update_key_page
    // would never observe the move. Walk BLink directly in that case.
    //
    // When affinity is off, no migrations happen, so the IndexCache is
    // authoritative and we keep the much cheaper hash lookup. This preserves
    // baseline (affinity-off) performance.
    std::ostringstream oss;
    oss << "OK " << accesses.size() << "\n";
    for (const auto& a : accesses) {
      Rid rid = enable_affinity
                    ? dtx->compute_server->get_rid_from_blink(
                          a.table_id, (itemkey_t)a.key)
                    : meta_man->Fetchrid(a.table_id, (itemkey_t)a.key);
      int64_t page = (int64_t)rid.page_no_;
      node_id_t owner = -1;
      if (page <= 0 || page > (int64_t)0x7fffffff) {
        page = -1;
      } else {
        owner = dtx->compute_server->get_node_id_by_tuple_id(
            a.table_id, (itemkey_t)a.key, (page_id_t)page);
      }
      oss << a.table_id << " " << a.key << " " << page << " " << owner
          << "\n";
    }
    SendNul(sock, oss.str());
  } catch (const std::exception& e) {
    if (dtx->tx_status != TXStatus::TX_COMMIT) {
      dtx->TxAbortWorkLoad(fake_yield);
    }
    SendNul(sock, "ABORT " + std::to_string(txn_id) +
                      " exception=" + std::string(e.what()));
  }
}

}  // namespace

bool TryHandleRouterCommand(int sock, const std::string& raw, DTX* dtx,
                            MetaManager* meta_man,
                            const std::string& bench_name) {
  if (!IsSmallBankBenchName(bench_name)) return false;
  // If the read pulled multiple commands in one shot, only handle the first.
  // MP-Router is synchronous (reads response before sending the next command),
  // so this is mostly a defense for ad-hoc nc testing.
  std::string head = raw;
  size_t nl = head.find('\n');
  if (nl != std::string::npos) head = head.substr(0, nl);
  std::string line = RTrim(head);
  if (line.empty()) return false;
  if (FirstTokenIs(line, "LOOKUP")) {
    HandleLookup(sock, Split(line), meta_man);
    return true;
  }
  if (FirstTokenIs(line, "SB")) {
    HandleSB(sock, Split(line), dtx, meta_man);
    return true;
  }
  return false;
}

}  // namespace router_protocol
