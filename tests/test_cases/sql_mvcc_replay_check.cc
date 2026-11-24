#include <fstream>
#include "common/fatal_checks.h"
#include "mvcc/row.h"
#include "mvcc/publication_rpc.h"

static undo::PageBytes ReadPage(const std::string& file, uint32_t page) {
    std::ifstream input(file, std::ios::binary);
    undo::PageBytes bytes{};
    input.seekg(uint64_t(page) * bytes.size());
    input.read(bytes.data(), bytes.size());
    CHECK(input.good()) << file << " page " << page;
    return bytes;
}

static undo::RowImage Previous(const undo::RowImage& current) {
    const auto& ref = current.previous;
    CHECK(!ref.empty());
    const auto page = ReadPage(undo::SpaceFile(ref.page), ref.page.number);
    CHECK(undo::Inspect(page).segment == current.version.creator);
    return undo::Read(page, ref, current.id).RestorePrevious(current, ref);
}

// Run in the fresh SQL database after the client's DDL replay barrier.
int main(int argc, char** argv) {
    EnableFatalChecks();
    const std::string table = "ycsb_user_table";
    const auto page0 = ReadPage(table, 0);
    RmFileHdr hdr;
    memcpy(&hdr, page0.data() + sizeof(RmPageHdr), sizeof(hdr));
    const auto page = ReadPage(table, 1);
    const auto current = mvcc::CopyRow(page.data(), hdr, {0, 1, 0, 0});
    const auto original = Previous(current);
    const auto absent = Previous(original);
    CHECK(!current.version.deleted && !original.version.deleted);
    CHECK(!current.version.initial && !original.version.initial);
    CHECK(absent.version.deleted && absent.version.initial && absent.previous.empty());
    const int current_magic = 80, original_magic = 125;
    std::string expected(sizeof(int) + 10 * 90, '\0');
    for (int field = 0; field < 10; ++field) {
        const auto value = "ycsb_0_" + std::to_string(field);
        expected.replace(sizeof(int) + field * 90, value.size(), value);
    }
    memcpy(expected.data(), &current_magic, sizeof(int));
    CHECK(current.value == expected);
    memcpy(expected.data(), &original_magic, sizeof(int));
    CHECK(original.value == expected);
    for (int i = 1; i < argc; ++i) {
        brpc::Channel channel;
        CHECK(channel.Init(argv[i], nullptr) == 0);
        const auto older = mvcc::SendStatusQuery(channel, original.version.creator);
        const auto newer = mvcc::SendStatusQuery(channel, current.version.creator);
        CHECK(older && newer && older->state == mvcc::State::Committed && newer->state == mvcc::State::Committed);
        CHECK(older->commit_ts > 0 && older->commit_ts < newer->commit_ts);
        std::cout << "commit timestamps at " << argv[i] << ": " << older->commit_ts << " " << newer->commit_ts << '\n';
    }
    std::cout << "PASS replayed SQL row and two undo references: 80 / 125 / absent\n";
}
