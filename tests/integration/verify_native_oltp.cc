// Verify a drained native benchmark heap against its committed transaction counts.
#include "base/data_item.h"
#include "record/record.h"
#include "oltp/schema.h"
#include <fstream>
#include <set>
#include <stdexcept>

namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template<class Value, class Visit>
void scan(const char* name, Visit visit) {
    std::ifstream file(name, std::ios::binary);
    RmFileHdr header;
    file.seekg(sizeof(RmPageHdr));
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    require(file.good() && header.record_size_ == sizeof(DataItem) + sizeof(Value), "bad heap layout");
    for (int p = 1; p < header.num_pages_; ++p) {
        std::array<char,PAGE_SIZE> bytes;
        file.seekg(std::streamoff(p) * PAGE_SIZE);
        file.read(bytes.data(), bytes.size());
        require(file.good(), "truncated heap page");
        RmPageHdr page;
        memcpy(&page, bytes.data(), sizeof(page));
        for (int i = 0; i < page.num_records_; ++i) {
            const size_t pos = sizeof(page) + header.bitmap_size_ + i * (sizeof(itemkey_t) + header.record_size_);
            require(pos + sizeof(itemkey_t) + header.record_size_ <= bytes.size(), "bad slot");
            itemkey_t key;
            Value value;
            memcpy(&key, bytes.data()+pos, sizeof(key));
            memcpy(&value, bytes.data()+pos+sizeof(key)+sizeof(DataItem), sizeof(value));
            visit(key,value,p);
        }
    }
}

void check_sysbench(size_t count, bool read_only) {
    std::ifstream manifest("sysbench_sbtest1.layout");
    std::string format;
    manifest >> format;
    int64_t span = 0;
    int nodes = 0, pages = 0;
    const bool modulo = format == "key-modulo-v1";
    if (modulo || format == "key-ring-v1") {
        size_t rows;
        int shuffled;
        manifest >> rows >> shuffled;
        if (!modulo) require(bool(manifest >> span) && span > 0, "bad legacy key range");
        require(bool(manifest >> nodes >> pages) && rows == count && nodes > 0 && pages > 0, "bad Sysbench layout");
    }
    auto owner = [&](int page) { return (page - 1) / pages % nodes; };
    std::vector<uint32_t> k(count);
    std::vector<int> page(count);
    size_t changed = 0;
    scan<oltp::SbRow>("sysbench_sbtest1", [&](auto id, const auto& row, int p) {
        require(id < count && page[id] == 0, "bad or duplicate primary key");
        k[id] = row.k;
        page[id] = p;
        auto initial = oltp::SbValue(uint32_t(id)*2654435761u);
        changed += row.k != initial.k || row.c != initial.c || row.pad != initial.pad;
    });
    std::set<uint32_t> indexed;
    scan<oltp::SbIndexLeaf>("sysbench_k_index", [&](auto key, const auto& leaf, int p) {
        require((nodes || key == unsigned(p)) && leaf.count > 0 && leaf.count <= leaf.entries.size(), "bad index leaf");
        require(std::is_sorted(leaf.entries.begin(), leaf.entries.begin()+leaf.count, oltp::IndexLess), "unsorted index");
        for (unsigned i=0; i<leaf.count; ++i) {
            const auto& e = leaf.entries[i];
            require(e.id<count && page[e.id] > 0 && k[e.id]==e.k && indexed.insert(e.id).second,
                    "secondary entry disagrees with heap");
            if (modulo) {
                require(key == oltp::SbLeafKey(e.id, nodes) && owner(p) == int(e.id % nodes) &&
                        owner(p) == owner(page[e.id]), "wrong modulo leaf or owner");
            } else if (span) {
                require(key == e.id - (e.id % span) % leaf.entries.size() && owner(p) == int(e.id / span % nodes) &&
                        owner(p) == owner(page[e.id]), "wrong legacy leaf or owner");
            } else {
                require(page[e.id] == p, "secondary leaf disagrees with heap page");
            }
        }
    });
    require(indexed.size() == count, "missing index entries");
    require((changed == 0) == read_only, "unexpected durable write footprint");
    std::cout << "Verified heap and index for " << count << " rows, changed rows=" << changed << '\n';
}

void check_tatp(size_t count, int delta, bool read_only) {
    std::vector<std::array<bool,4>> facility(count);
    std::vector<int> home(count);
    for (int table=0; table<4; ++table) {
        const char* names[] = {"tatp_subscriber","tatp_access_info","tatp_special_facility","tatp_call_forwarding"};
        std::vector<bool> seen(count);
        int forwarding_delta = 0;
        scan<oltp::TatpRow>(names[table], [&](auto id, const auto& row, int page) {
            require(id<count && !seen[id], "bad or duplicate subscriber key");
            seen[id] = true;
            if (!table) home[id] = page;
            require(home[id] == page, "subscriber tables are not colocated");
            const auto initial = oltp::TatpInitial(table,id);
            if (read_only || table == 1) require(!memcmp(&row,&initial,sizeof(row)), "unexpected row change");
            if (table == 0)
                require(!memcmp(row.subscriber.number,initial.subscriber.number,16), "wrong telephone number");
            for (int t=0; t<4; ++t) {
                if (table == 2) {
                    facility[id][t] = row.facility[t].present;
                    require(facility[id][t] == initial.facility[t].present, "facility membership changed");
                } else if (table == 3) {
                    for (int s=0; s<3; ++s) {
                        auto end = row.forwarding[t][s].end_hour;
                        require(end <= 24 && (!end || facility[id][t]), "orphan or invalid forwarding slot");
                        forwarding_delta += int(end != 0) - int(initial.forwarding[t][s].end_hour != 0);
                    }
                }
            }
        });
        require(std::count(seen.begin(), seen.end(), true) == count, "missing subscribers");
        if (table == 3) require(forwarding_delta == delta, "durable forwarding count differs from committed inserts minus deletes");
    }
    std::cout << "Verified four tables for " << count << " subscribers, net forwarding change=" << delta << '\n';
}
}

int main(int argc, char** argv) {
    if (argc != 5) return 2;
    const auto size = std::stoul(argv[2]);
    const auto delta = std::stoi(argv[3]);
    const bool read_only = std::stoi(argv[4]);
    if (std::string(argv[1]) == "tatp") check_tatp(size,delta,read_only);
    else if (std::string(argv[1]) == "sysbench") check_sysbench(size,read_only);
    else return 2;
}
