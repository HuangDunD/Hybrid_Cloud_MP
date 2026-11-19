#include "fsm_tree.h"
#include "compute_server/server.h"

namespace {

// The frame already contains the node array as bytes. Decode just the header
// and the selected child ID (the latter need not be naturally aligned).
class PinnedPage {
public:
    PinnedPage(ComputeServer* server, table_id_t table, uint32_t id, bool write)
        : server_(server), table_(table), id_(id), write_(write) {
        if (SYSTEM_MODE == 0)
            page_ = write ? server_->rpc_fetch_x_page(table, id) : server_->rpc_fetch_s_page(table, id);
        else
            page_ = write ? server_->rpc_lazy_fetch_x_page(table, id) : server_->rpc_lazy_fetch_s_page(table, id);
    }
    PinnedPage(const PinnedPage&) = delete;
    ~PinnedPage() {
        if (SYSTEM_MODE == 0) {
            if (write_) server_->rpc_release_x_page(table_, id_);
            else server_->rpc_release_s_page(table_, id_);
        } else {
            if (write_) server_->rpc_lazy_release_x_page(table_, id_);
            else server_->rpc_lazy_release_s_page(table_, id_);
        }
    }
    char* bytes() const { return page_->get_data(); }
    void changed() { page_->set_dirty(true); }

private:
    ComputeServer* server_;
    table_id_t table_;
    uint32_t id_;
    bool write_;
    Page* page_;
};

class NodePage : public PinnedPage {
public:
    NodePage(ComputeServer* server, table_id_t table, uint32_t id, bool write = false)
        : PinnedPage(server, table, id, write) {
        memcpy(&hdr, bytes(), sizeof(hdr));
        CHECK(hdr.magic_number == 0x46535047 && hdr.page_id == id);
        CHECK(hdr.node_count <= PAGE_SIZE - sizeof(hdr) && hdr.leaf_start < hdr.node_count);
        CHECK(hdr.page_type == S_FSMPageType::LEAF_PAGE || hdr.page_type == S_FSMPageType::INTERNAL_PAGE);
        CHECK((hdr.page_type == S_FSMPageType::LEAF_PAGE ? hdr.heap_pages_count : hdr.child_count)
              == hdr.node_count - hdr.leaf_start);
        nodes = reinterpret_cast<uint8_t*>(bytes() + sizeof(hdr));
    }

    uint32_t child(uint32_t slot) const {
        CHECK(slot < hdr.child_count);
        if (hdr.child_count > (PAGE_SIZE - sizeof(hdr) - hdr.node_count) / sizeof(uint32_t))
            return hdr.first_child_page + slot; // older implicit child array
        uint32_t id;
        memcpy(&id, nodes + hdr.node_count + slot * sizeof(id), sizeof(id));
        return id;
    }

    uint32_t search(uint8_t need) const {
        if (nodes[0] < need) return UINT32_MAX;
        uint32_t i = 0;
        while (i < hdr.leaf_start) {
            i = 2 * i + 1;
            CHECK(i < hdr.node_count);
            if (nodes[i] < need) ++i;
            CHECK(i < hdr.node_count && nodes[i] >= need);
        }
        return i - hdr.leaf_start;
    }

    bool set(uint32_t slot, uint8_t value) {
        uint32_t i = hdr.leaf_start + slot;
        CHECK(i < hdr.node_count);
        const uint8_t old_root = nodes[0];
        while (nodes[i] != value) {
            nodes[i] = value;
            changed();
            if (i == 0) break;
            i = (i - 1) / 2;
            uint32_t left = 2 * i + 1;
            value = nodes[left];
            if (left + 1 < hdr.node_count) value = std::max(value, nodes[left + 1]);
        }
        return old_root != nodes[0];
    }

    S_FSMPageHeader hdr;
    uint8_t* nodes;
};
}  // namespace

SecFSM::SecFSM(ComputeServer* server, table_id_t table) : server_(server), table_(table) {}

void SecFSM::load_metadata() {
    std::call_once(init_, [&] {
        PinnedPage page(server_, table_, S_FSM_META_PAGE_ID, false);
        memcpy(&meta_, page.bytes(), sizeof(meta_));
        CHECK(meta_.magic_number == 0x46534D54 && meta_.table_id == table_);
        CHECK(meta_.version == 1 || meta_.version == fsm::kVersion);
        CHECK(meta_.leaves_per_page == fsm::kLeafSlots && meta_.tree_height > 0);
    });
}

bool SecFSM::initialize(table_id_t table) {
    CHECK(table == INVALID_TABLE_ID || table == table_);
    load_metadata();
    return true;
}

uint32_t SecFSM::leaf_for(uint32_t heap_page) const {
    uint32_t leaf = heap_page / meta_.leaves_per_page;
    if (meta_.version == fsm::kVersion) return fsm::PageNumber(0, leaf);
    return meta_.next_fsm_page_id - meta_.total_fsm_pages + 1 + leaf;
}

uint32_t SecFSM::find_free_page(uint32_t min_space_needed) {
    const uint8_t need = FsmSpaceCategory(std::max(1u, min_space_needed));
    const uint64_t hint = candidate_.load(std::memory_order_relaxed);
    if (uint8_t(hint) >= need) return uint32_t(hint >> 8);
    load_metadata();
    uint32_t id = meta_.root_page_id;
    for (;;) {
        NodePage page(server_, table_, id);
        uint32_t slot = page.search(need);
        if (slot == UINT32_MAX) return UINT32_MAX;
        if (page.hdr.page_type == S_FSMPageType::LEAF_PAGE) {
            uint32_t heap = page.hdr.first_heap_page + slot;
            return heap > 0 && heap < meta_.total_heap_pages ? heap : UINT32_MAX;
        }
        id = page.child(slot);
    }
}

void SecFSM::update_page_space(uint32_t heap, uint32_t bytes) {
    load_metadata();
    if (!heap || heap >= meta_.total_heap_pages) {
        LOG(ERROR) << "heap page " << heap << " outside FSM for table " << table_;
        return;
    }
    const uint32_t id = leaf_for(heap);
    const uint8_t category = FsmSpaceCategory(bytes);
    uint32_t parent;
    uint8_t root;
    bool root_changed;
    {
        NodePage leaf(server_, table_, id, true);
        CHECK(leaf.hdr.page_type == S_FSMPageType::LEAF_PAGE && heap >= leaf.hdr.first_heap_page);
        root_changed = leaf.set(heap - leaf.hdr.first_heap_page, category);
        root = leaf.nodes[0];
        parent = leaf.hdr.parent_page;
    }
    if (root_changed && parent) publish_root(heap, id, parent, root);
    candidate_.store((uint64_t(heap) << 8) | category, std::memory_order_relaxed);
}

void SecFSM::publish_root(uint32_t heap, uint32_t child, uint32_t parent, uint8_t value) {
    // Hold one frame at a time, including with a one-frame FSM pool. Another
    // writer may publish a newer root between our release and parent update.
    // Recheck after publishing so the last publisher leaves a current summary.
    for (;;) {
        uint32_t grandparent;
        uint8_t root;
        bool root_changed;
        {
            NodePage page(server_, table_, parent, true);
            CHECK(page.hdr.level > 0 && page.hdr.level <= fsm::kRootLevel);
            uint32_t slot = fsm::ChildSlot(heap, page.hdr.level);
            CHECK(page.child(slot) == child);
            root_changed = page.set(slot, value);
            grandparent = page.hdr.parent_page;
            root = page.nodes[0];
        }
        if (root_changed && grandparent) publish_root(heap, parent, grandparent, root);
        NodePage current(server_, table_, child);
        if (current.nodes[0] == value) return;
        value = current.nodes[0];
    }
}

uint32_t SecFSM::get_page_space(uint32_t heap) {
    load_metadata();
    if (!heap || heap >= meta_.total_heap_pages) return 0;
    uint32_t id = meta_.root_page_id;
    while (id != leaf_for(heap)) {
        NodePage page(server_, table_, id);
        CHECK(page.hdr.level > 0 && page.hdr.level <= fsm::kRootLevel);
        uint32_t slot = fsm::ChildSlot(heap, page.hdr.level);
        if (!page.nodes[page.hdr.leaf_start + slot]) return 0;
        id = page.child(slot);
    }
    NodePage page(server_, table_, id);
    uint32_t slot = heap - page.hdr.first_heap_page;
    CHECK(slot < page.hdr.heap_pages_count);
    switch (page.nodes[page.hdr.leaf_start + slot]) {
        case 255: return PAGE_SIZE * 9 / 10;
        case 192: return PAGE_SIZE * 2 / 3;
        case 128: return PAGE_SIZE / 3;
        case 64: return PAGE_SIZE / 10;
        default: return 0;
    }
}
