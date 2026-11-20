#include "undo/store.h"
#include <algorithm>
#include <sys/stat.h>
#include <butil/logging.h>

namespace undo {
namespace {
PageBytes SpaceHeader(PageKey key) {
    key.number = 1;
    PageBytes page{};
    std::copy_n("UNDOSPC1", 8, page.data());
    const auto encoded = EncodeKey(key);
    std::copy(encoded.begin(), encoded.end(), page.begin() + 8);
    return page;
}
std::string Bytes(const Redo& r) {
    std::string out(r.log_tot_len_, '\0');
    r.serialize(out.data());
    return out;
}
}  // namespace
Store::~Store() {
    for (const auto& file : files_) disk_.close_file(file.second);
}
std::shared_ptr<Store::Progress> Store::Get(const PageKey& key) {
    Validate(key);
    std::lock_guard<std::mutex> lock(map_mutex_);
    const auto found = progress_.find(key);
    if (found != progress_.end()) return found->second;
    return progress_.emplace(key, std::make_shared<Progress>()).first->second;
}
int Store::File(const PageKey& key, bool create) {
    std::lock_guard<std::mutex> lock(file_mutex_);
    const auto name = SpaceFile(key);
    auto known = files_.find(name);
    if (known != files_.end()) return known->second;
    const bool exists = disk_.is_file(name);
    if (!exists && !create) return -1;  // a published redo dependency can still be pending
    if (!exists) disk_.create_file(name);
    const int fd = disk_.open_file(name);
    CHECK(fd >= 0);
    const auto expected = SpaceHeader(key);
    if (!exists)
        disk_.write_page(fd, 0, expected.data(), PAGE_SIZE);
    else {
        PageBytes actual{};
        disk_.read_page(fd, 0, actual.data(), PAGE_SIZE);
        CHECK(actual == expected) << "undo space identity/format";
    }
    files_.emplace(name, fd);
    return fd;
}
bool Store::Load(const PageKey& key, PageBytes& page, Header* header) {
    const int fd = File(key, false);
    if (fd < 0) return false;
    struct stat st{};
    const int result = fstat(fd, &st);
    CHECK(result == 0);
    if (uint64_t(st.st_size) < (uint64_t(key.number) + 1) * PAGE_SIZE) return false;
    disk_.read_page(fd, key.number, page.data(), PAGE_SIZE);
    if (std::all_of(page.begin(), page.end(), [](char c) { return c == 0; }))
        return false;  // allocated file hole, not a chain endpoint
    const auto h = Inspect(page);
    CHECK(h.key == key);
    if (header) *header = h;
    return true;
}
uint64_t Store::Submit(std::shared_ptr<Redo> redo) {
    auto state = Get(redo->page);
    std::unique_lock<bthread::Mutex> lock(state->mutex);
    PageBytes page{};
    Header loaded_header;
    state->exists = Load(redo->page, page, &loaded_header);
    state->loaded = true;
    state->lsn = state->exists ? loaded_header.lsn : 0;
    if (state->exists && redo->lsn_ <= state->lsn) {
        redo->Apply(page, true);
        return 1;
    }
    CHECK(redo->prev_lsn_ >= state->lsn && redo->lsn_ > redo->prev_lsn_);
    auto& siblings = state->pending[redo->prev_lsn_];
    if (!siblings.empty()) CHECK(Bytes(*siblings.front()) == Bytes(*redo)) << "forked undo page WAL chain";
    siblings.push_back(std::move(redo));
    uint64_t applied = 0;
    while (true) {
        auto ready = state->pending.find(state->lsn);
        if (ready == state->pending.end()) break;
        auto logs = std::move(ready->second);
        state->pending.erase(ready);
        for (const auto& log : logs) {
            log->Apply(page, state->exists);
            state->exists = true;
            ++applied;
        }
        const auto header = Inspect(page);
        state->lsn = header.lsn;
        const int fd = File(header.key, true);
        disk_.write_page(fd, header.key.number, page.data(), PAGE_SIZE);
    }
    lock.unlock();
    if (applied) state->changed.notify_all();
    return applied;
}
PageBytes Store::ReadPage(const PageKey& key, uint64_t required) {
    CHECK(required > 0);
    auto state = Get(key);
    std::unique_lock<bthread::Mutex> lock(state->mutex);
    PageBytes bytes{};
    Header header;
    if (!state->loaded) {
        state->exists = Load(key, bytes, &header);
        state->lsn = state->exists ? header.lsn : 0;
        state->loaded = true;
        if (state->lsn >= required) return bytes;
    }
    if (state->lsn < required) {
        {
            std::lock_guard<std::mutex> event(observer_mutex_);
            ++waits_;
            observer_.notify_all();
        }
        while (state->lsn < required) state->changed.wait(lock);
    }
    const bool exists = Load(key, bytes, &header);
    CHECK(exists && header.lsn >= required) << "replayed undo page missing";
    return bytes;
}
bool Store::Ready(const Ref& ref, const RowId& row, const mvcc::TxnId& creator,
                  std::string_view successor) {
    Validate(ref);
    CHECK(!ref.empty());
    auto state = Get(ref.page);
    std::lock_guard<bthread::Mutex> lock(state->mutex);
    if (state->loaded && state->lsn < ref.lsn) return false;
    PageBytes bytes{};
    Header header;
    const bool exists = Load(ref.page, bytes, &header);
    if (!state->loaded) {
        state->exists = exists;
        state->lsn = exists ? header.lsn : 0;
        state->loaded = true;
        if (state->lsn < ref.lsn) return false;
    }
    CHECK(exists && header.segment == creator) << "undo missing or belongs to a different creator";
    const auto record = undo::Read(bytes, ref, row);
    record.CheckSuccessor(successor);
    return true;
}
void Store::Sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    for (const auto& file : files_) {
        const int result = fdatasync(file.second);
        CHECK(result == 0) << "undo replay drain sync";
    }
}
bool Store::WaitForReadWaits(uint64_t count, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(observer_mutex_);
    return observer_.wait_for(lock, timeout, [&] { return waits_.load() >= count; });
}
}  // namespace undo
