#include "mvcc/row.h"
#include "util/bitmap.h"
namespace mvcc {
static_assert(sizeof(RowFields{}.history) == undo::kRefBytes);
DataItem* RowAt(char* data, const RmFileHdr& file, const undo::RowId& id) {
    file.CheckFormat();
    CHECK(id.page > 0 && id.slot < uint32_t(file.num_records_per_page_));
    const size_t offset =
        sizeof(RmPageHdr) + file.bitmap_size_ + id.slot * (file.record_size_ + sizeof(itemkey_t));
    CHECK(offset + sizeof(itemkey_t) + file.record_size_ <= PAGE_SIZE);
    CHECK(Bitmap::is_set(data + sizeof(RmPageHdr), id.slot)) << "MVCC must retain the physical slot";
    itemkey_t key;
    memcpy(&key, data + offset, sizeof(key));
    CHECK(key == id.key);
    auto* item = reinterpret_cast<DataItem*>(data + offset + sizeof(key));
    CHECK(item->table_id == int(id.table) && item->value_size > 0 &&
          size_t(file.record_size_) == sizeof(DataItem) + item->value_size);
    CHECK(item->mvcc.format == 0x4d564332 && item->mvcc.reserved == 0 && item->mvcc.initial <= 1 &&
          item->valid <= 1);
    return item;
}
namespace {
undo::RowImage RowFromValue(const DataItem& item, const char* value, const undo::RowId& id) {
    undo::RowImage row;
    row.id = id;
    row.version.initial = item.mvcc.initial;
    row.version.deleted = !item.valid;
    if (row.version.initial) {
        const std::array<char, 16> empty{};
        CHECK(item.mvcc.creator_local == 0 && item.mvcc.creator_home == 0 &&
              item.mvcc.creator_epoch == empty);
    } else
        row.version.creator = {std::string(item.mvcc.creator_epoch.data(), 16), item.mvcc.creator_home,
                               item.mvcc.creator_local};
    row.previous = undo::DecodeRef({item.mvcc.history.data(), item.mvcc.history.size()});
    CHECK(row.version.initial == row.previous.empty()) << "P3 existing-row lineage";
    CHECK(item.value_size > 0);
    row.value.assign(value, item.value_size);
    return row;
}
}  // namespace
undo::RowImage RowImageFromItem(const DataItem& item, const undo::RowId& id) {
    // Heap/serialized items carry their value behind the item->value pointer;
    // it is not necessarily contiguous with the DataItem struct.
    CHECK(item.value != nullptr);
    return RowFromValue(item, reinterpret_cast<const char*>(item.value), id);
}
undo::RowImage CopyRow(const char* data, const RmFileHdr& file, const undo::RowId& id) {
    const auto* disk = RowAt(const_cast<char*>(data), file, id);
    auto row = RowFromValue(*disk, reinterpret_cast<const char*>(disk) + sizeof(DataItem), id);
    const auto encoded = undo::EncodeRow(row);
    (void)encoded;
    return row;
}
void SetFields(DataItem& item, const undo::RowImage& row) {
    item.valid = !row.version.deleted;
    item.mvcc = RowFields{};
    item.mvcc.initial = row.version.initial;
    if (!row.version.initial) {
        undo::Validate(row.version.creator);
        item.mvcc.creator_home = row.version.creator.home;
        item.mvcc.creator_local = row.version.creator.local;
        memcpy(item.mvcc.creator_epoch.data(), row.version.creator.epoch.data(), 16);
    }
    const auto ref = undo::EncodeRef(row.previous);
    memcpy(item.mvcc.history.data(), ref.data(), ref.size());
}
void InstallRow(DataItem& disk, const undo::RowImage& row) {
    CHECK(disk.table_id == int(row.id.table) && disk.value_size == int(row.value.size()));
    SetFields(disk, row);
    memcpy(reinterpret_cast<char*>(&disk) + sizeof(DataItem), row.value.data(), row.value.size());
}
void CopyToItem(DataItem& item, const undo::RowImage& row) {
    DataItem copy(row.id.table, row.value.size());
    memcpy(copy.value, row.value.data(), row.value.size());
    SetFields(copy, row);
    item = copy;
}
}  // namespace mvcc
