#pragma once
#include "catalog.h"
#include "schema.h"
#include "compute_server/server.h"
#include "dtx/dtx.h"
#include "util/json_config.h"
#include "util/zipfan.h"
#include <map>

namespace oltp {
struct Outcome {
    int type = 0;
    bool committed = false, miss = false, local = true;
    unsigned writers = 0;
};

class Generator {
public:
    Generator(const std::string& name, const JsonConfig& cfg, MetaManager* meta,
              ComputeServer* server, uint64_t seed)
        : tatp(name == "tatp"), home(server->getNodeID()), random(seed) {
        const auto configured_size = cfg.get(tatp ? "subscribers" : "table_size").get_int64();
        const auto configured_range = tatp ? 1 : cfg.get("range_size").get_int64();
        CHECK(configured_size > 0 && configured_size <= INT32_MAX);
        CHECK(configured_range > 0 && configured_range <= configured_size);
        CHECK(WR_TXN_RATE >= 0 && WR_TXN_RATE <= 1);
        CHECK(LOCAL_TRASACTION_RATE >= 0 && LOCAL_TRASACTION_RATE <= 1);
        size = configured_size;
        range = configured_range;
        if (tatp) {
            distribution = cfg.get("key_distribution").get_str("system");
            CHECK(distribution == "system" || distribution == "uniform" || distribution == "nurand");
        }
        owner.assign(size, -1);
        if (!tatp) index_keys.resize(size);
        points.resize(ComputeNodeCount);
        ranges.resize(ComputeNodeCount);
        for (int n=0; n<ComputeNodeCount; ++n) {
            for (auto key : meta->GetNodeKeys(0,n)) {
                CHECK(key < owner.size() && owner[key] == -1);
                owner[key] = n;
                if (!tatp) {
                    auto leaf = meta->Fetchrid(0,key).page_no_;
                    const auto rid = meta->Fetchrid(1,leaf);
                    CHECK(rid.page_no_ == leaf && server->get_node_id_by_page_id(1,leaf) == n);
                    index_keys[key] = leaf;
                } else {
                    for (int t=1; t<4; ++t) {
                        const auto rid = meta->Fetchrid(t,key);
                        CHECK(rid.page_no_ > 0 && server->get_node_id_by_page_id(t,rid.page_no_) == n);
                    }
                }
            }
        }
        // Walk backward to find every start whose whole logical interval is local.
        int contiguous = 0, previous = -1;
        for (int id=size-1; id>=0; --id) {
            int n = Owner(id);
            CHECK(n >= 0) << "Incomplete storage index";
            contiguous = n == previous ? contiguous+1 : 1;
            previous = n;
            points[n].push_back(id);
            if (!tatp && contiguous >= range) ranges[n].push_back(id);
        }
        zipf = cfg.get("use_zipfian").get_int64() == 1;
        hot_percent = cfg.get("hot_percent").get_int64();
        auto skew = cfg.get("zipf_theta");
        double theta = skew.is_int64() ? skew.get_int64() : skew.get_double();
        CHECK(hot_percent >= 0 && hot_percent <= 100);
        for (int n=0; n<ComputeNodeCount; ++n) {
            CHECK(!points[n].empty()) << "Increase workload size or reduce partition_size_per_table";
            std::reverse(points[n].begin(), points[n].end());
            std::reverse(ranges[n].begin(), ranges[n].end());
            if (!tatp && n == home && LOCAL_TRASACTION_RATE > 0)
                CHECK(!ranges[n].empty()) << "range_size exceeds the local contiguous span";
            point_gen.emplace_back(points[n].size(), theta, Next());
            range_gen.emplace_back(std::max<size_t>(1,ranges[n].size()), theta, Next());
        }
    }
    Outcome Run(DTX& dtx, tx_id_t tx) {
        Outcome out;
        const bool write = Chance(WR_TXN_RATE);
        const bool local = Chance(LOCAL_TRASACTION_RATE);
        dtx.TxBegin(tx);
        if (tatp) Tatp(dtx, write, local, out);
        else Sysbench(dtx, write, local, out);
        return out;
    }
private:
    bool tatp, zipf;
    int size, range, home, hot_percent;
    uint64_t random, checksum = 0;
    std::string distribution = "system";
    std::vector<int> owner, index_keys;
    std::vector<std::vector<int>> points, ranges;
    std::vector<ZipFanGen> point_gen, range_gen;

    uint32_t Next() { return FastRand(&random); }
    bool Chance(double probability) { return Next() < probability * 4294967296.0; }
    int Owner(int id) const { return owner[id]; }

    int Pick(DTX& dtx, bool local, bool interval = false) {
        const int node = local ? home : int(Next()%ComputeNodeCount);
        const bool restrict_range = local && interval;
        const auto& keys = restrict_range ? ranges[node] : points[node];
        auto& generator = restrict_range ? range_gen[node] : point_gen[node];
        const size_t hot = std::min(keys.size(), size_t(std::max(1, int(HOT_KEY_TOP_N))));
        size_t rank;
        if (distribution != "system") {
            const uint32_t mask = size <= 1000000 ? 65535 : size <= 10000000 ? 1048575 : 2097151;
            auto sample = [&](size_t n) {
                size_t r = Next()%n;
                if (distribution == "nurand") r = (r | (Next() & mask)) % n;
                return r;
            };
            if (!local) {
                const auto id = sample(size);
                dtx.NoteKeyAccess(id < hot);
                return id;
            }
            rank = sample(keys.size());
        } else if (zipf) rank = generator.next() % keys.size();
        else if (hot == keys.size() || Next()%100 < unsigned(hot_percent)) rank = Next()%hot;
        else rank = hot + Next()%(keys.size()-hot);
        dtx.NoteKeyAccess(rank < hot);
        return keys[rank];
    }
    static DataItemPtr AccessRow(DTX& dtx, int table, itemkey_t key, bool write) {
        auto item = std::make_shared<DataItem>(table);
        if (write) dtx.AddToReadWriteSet(item,key);
        else dtx.AddToReadOnlySet(item,key);
        return item;
    }
    template<class T> static T& Value(const DataItemPtr& item) {
        CHECK(item && item->valid && item->value && item->value_size == sizeof(T));
        return *reinterpret_cast<T*>(item->value);
    }

    void Tatp(DTX& dtx, bool write, bool local, Outcome& out) {
        const int id = Pick(dtx, local);
        const auto draw = Next()%(write ? 20 : 80);
        out.type = write ? (draw < 2 ? 3 : draw < 16 ? 4 : draw < 18 ? 5 : 6)
                         : (draw < 35 ? 0 : draw < 45 ? 1 : 2);
        const int type = Next()%4, slot = Next()%3, end_hour = 1+Next()%24;
        std::array<DataItemPtr,4> items;
        // Read and write masks in Subscriber, Access_Info, Special_Facility, Call_Forwarding order.
        static constexpr unsigned read_mask[] = {1,12,2,0,0,5,1};
        static constexpr unsigned write_mask[] = {0,0,0,5,1,8,8};
        auto value = [&](int table) -> TatpRow& { return Value<TatpRow>(items[table]); };
        uint8_t subscriber_bit = 0;
        auto update = [&](int table, TatpRow& row) {
            switch (out.type) {
            case 3:
                // The facility predicate controls both updates. Evaluate it first.
                if (table == 2) {
                    out.miss = !row.facility[type].present;
                    if (!out.miss) {
                        subscriber_bit = Next()%2;
                        row.facility[type].data = Next();
                    }
                } else if (!out.miss) row.subscriber.bit[0] = subscriber_bit;
                break;
            case 4: row.subscriber.location = Next(); break;
            case 5: {
                auto& f = row.forwarding[type][slot];
                out.miss = !value(2).facility[type].present || f.end_hour;
                if (!out.miss) { f.end_hour = end_hour; Phone(f.number, Next()%size); }
                break;
            }
            case 6: {
                auto& f = row.forwarding[type][slot];
                out.miss = !f.end_hour;
                if (!out.miss) f = {};
                break;
            }
            default: CHECK(false);
            }
        };
        std::array<int,4> tables{0, 1, 2, 3};
        if (out.type == 3) std::swap(tables[0], tables[2]);
        for (int t : tables) {
            const auto bit = 1u << t;
            if (read_mask[out.type] & bit) items[t] = AccessRow(dtx,t,id,false);
            if (write_mask[out.type] & bit) {
                items[t] = std::make_shared<DataItem>(t);
                dtx.AddToReadWriteSet(items[t], id, [&, t](DataItem& item) {
                    CHECK(item.valid && item.value_size == sizeof(TatpRow));
                    update(t, *reinterpret_cast<TatpRow*>(item.value));
                });
            }
        }
        out.local = Owner(id) == home;
        out.writers = write ? 1 : 0;
        dtx.DecideCommitMode();
        if (!dtx.TxExe(true, DTX::FetchOrder::IndependentRows)) return;
        if (items[0]) {
            char number[16];
            Phone(number,id);
            CHECK(std::memcmp(value(0).subscriber.number, number, 16) == 0);
        }
        switch (out.type) {
        case 0: checksum += value(0).subscriber.location; break;
        case 1: {
            out.miss = true;
            if (value(2).facility[type].active) {
                for (int i=0; i<=slot; ++i) {
                    const auto& f = value(3).forwarding[type][i];
                    if (f.end_hour > end_hour) { out.miss = false; checksum += f.number[14]; }
                }
            }
            break;
        }
        case 2: out.miss = !value(1).access[type].present; checksum += value(1).access[type].data1; break;
        }
        if (out.miss) dtx.TxAbortWorkLoad();
        else out.committed = dtx.TxCommit();
    }

    void Sysbench(DTX& dtx, bool write, bool local, Outcome& out) {
        out.type = write ? 1 : 0;
        std::array<int,10> point_ids;
        std::array<int,4> starts;
        std::array<int,3> writes{};
        std::map<int,DataItemPtr> heap, leaves;
        std::set<int> writable, writers;
        auto plan = [&](int id) {
            heap.emplace(id,nullptr);
            out.local &= Owner(id) == home;
        };
        for (auto& id : point_ids) { id = Pick(dtx,local); plan(id); }
        auto length = [&](int first) { return std::min(range, size-first); };
        for (auto& first : starts) {
            first = Pick(dtx,local,true);
            for (int i=0; i<length(first); ++i) plan(first+i);
        }
        if (write) for (auto& id : writes) {
            id = Pick(dtx,local);
            plan(id);
            writable.insert(id);
            writers.insert(Owner(id));
        }
        out.writers = writers.size();
        for (auto& [id,item] : heap) item = AccessRow(dtx,0,id,writable.count(id));
        if (write) {
            leaves.emplace(index_keys[writes[0]], nullptr);
            leaves.emplace(index_keys[writes[2]], nullptr);
            for (auto& [page,item] : leaves) item = AccessRow(dtx,1,page,true);
        }
        dtx.DecideCommitMode();
        if (!dtx.TxExe()) return;
        auto row = [&](int id) -> SbRow& { return Value<SbRow>(heap.at(id)); };
        auto change_index = [&](int id, uint32_t next) {
            auto& leaf = Value<SbIndexLeaf>(leaves.at(index_keys[id]));
            CHECK(leaf.Replace(id, row(id).k, next)) << "Secondary index disagrees with heap row " << id;
        };
        for (auto id : point_ids) checksum += row(id).c[0];
        for (size_t op=0; op<starts.size(); ++op) {
            std::vector<std::array<char,120>> values;
            for (int i=0; i<length(starts[op]); ++i) {
                const auto& r = row(starts[op]+i);
                if (op == 0) checksum += r.c[0];
                else if (op == 1) checksum += r.k;
                else values.push_back(r.c);
            }
            if (!values.empty()) {
                std::sort(values.begin(),values.end());
                if (op == 3) values.erase(std::unique(values.begin(),values.end()),values.end());
                checksum += values.front()[0] + values.size();
            }
        }
        if (write) {
            change_index(writes[0],row(writes[0]).k+1);
            ++row(writes[0]).k;
            row(writes[1]).c = SbValue(Next()).c;
            // Upstream deletes and reinserts the same id inside one transaction.
            auto replacement = SbValue(Next());
            change_index(writes[2],replacement.k);
            row(writes[2]) = replacement;
        }
        out.committed = dtx.TxCommit();
    }
public:
    uint64_t Checksum() const { return checksum; }
};
}
