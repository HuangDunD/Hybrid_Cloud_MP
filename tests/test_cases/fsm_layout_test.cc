#include "storage/fsm_tree/s_fsm_tree.h"
#include <filesystem>

namespace {
void CheckPage(DiskManager& disk, int fd, uint32_t leaf, unsigned level) {
    const auto index = leaf / fsm::LeafSpan(level);
    const auto id = fsm::PageNumber(level, index);
    char bytes[PAGE_SIZE];
    disk.read_page(fd, id, bytes, sizeof(bytes));
    S_FSMPageHeader hdr{};
    memcpy(&hdr, bytes, sizeof(hdr));
    CHECK_EQ(hdr.page_id, id);
    CHECK_EQ(hdr.level, level);
    CHECK_EQ(hdr.parent_page, level == fsm::kRootLevel ? 0 : fsm::PageNumber(level + 1, index / fsm::kFanout));
    if (level == 0) {
        CHECK_EQ(hdr.first_heap_page, leaf * fsm::kLeafSlots);
        CHECK_EQ(hdr.heap_pages_count, fsm::kLeafSlots);
    } else {
        uint32_t slot = fsm::ChildSlot(leaf * fsm::kLeafSlots, level);
        CHECK_LT(slot, hdr.child_count);
        uint32_t child;
        memcpy(&child, bytes + sizeof(hdr) + hdr.node_count + slot * sizeof(child), sizeof(child));
        CHECK_EQ(child, fsm::PageNumber(level - 1, leaf / fsm::LeafSpan(level - 1)));
    }
}
}  // namespace

int main() {
    char dir[] = "/tmp/sql-fsm-test-XXXXXX";
    CHECK(mkdtemp(dir));
    const auto previous = std::filesystem::current_path();
    std::filesystem::current_path(dir);
    DiskManager disk;
    StorageBufferPoolManager pool(8, &disk);
    disk.create_file("t_fsm");
    int fd = disk.open_file("t_fsm");
    {
        S_SecFSM tree(&disk, &pool, 20000, "sql");
        tree.set_custom_filename("t_fsm");
        CHECK(tree.initialize(20000, 1)); // header-only heap
        CHECK_EQ(tree.find_free_page(1), UINT32_MAX);
        tree.update_page_space(1, PAGE_SIZE / 2);
        CHECK_EQ(tree.find_free_page(1), 1u);
    }
    char prefix[PAGE_SIZE];
    disk.read_page(fd, fsm::PageNumber(0, 0), prefix, sizeof(prefix));
    // Cross both the first leaf and the first 512-leaf parent boundary.
    for (uint32_t leaf = 1; leaf <= fsm::kFanout; ++leaf)
        S_SecFSM::AppendSqlLeaf(disk, fd, leaf);
    for (uint32_t leaf : {0u, 1u, fsm::kFanout - 1, fsm::kFanout})
        for (unsigned level = 0; level <= fsm::kRootLevel; ++level)
            CheckPage(disk, fd, leaf, level);
    char after[PAGE_SIZE];
    disk.read_page(fd, fsm::PageNumber(0, 0), after, sizeof(after));
    CHECK_EQ(memcmp(prefix, after, sizeof(after)), 0);
    disk.close_file(fd);

    disk.create_file("ycsb_user_table_fsm");
    {
        S_SecFSM legacy(&disk, &pool, 20000, "ycsb");
        CHECK(legacy.initialize(20000, 1025));
        legacy.update_page_space(1024, PAGE_SIZE / 2);
        CHECK_EQ(legacy.find_free_page(1), 1024u);
        legacy.update_page_space(1024, 0);
        CHECK_EQ(legacy.find_free_page(1), UINT32_MAX);
    }
    disk.close_file(disk.open_file("ycsb_user_table_fsm"));
    std::filesystem::current_path(previous);
    std::filesystem::remove_all(dir);
}
