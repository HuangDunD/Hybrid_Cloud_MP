#include "scan.h"
#include "mvcc/row.h"

Scan::Scan(DTX* dtx, table_id_t table, std::optional<itemkey_t> point)
    : m_dtx(dtx), m_fileHdr(dtx->compute_server->get_file_hdr(table)),
      m_rid{1, -1}, m_key(0), m_tableID(table), point_lookup(point.has_value()) {
    if (!point_lookup) {
        next();
        return;
    }
    m_key = *point;
    m_rid = dtx->compute_server->get_rid_from_blink(table, m_key);
    if (m_rid.page_no_ == INVALID_PAGE_ID) return;
    auto* data = dtx->compute_server->FetchSPage(table, m_rid.page_no_);
    auto row = mvcc::CopyRow(data, *m_fileHdr,
        {uint32_t(table), uint32_t(m_rid.page_no_), uint32_t(m_rid.slot_no_), m_key});
    dtx->compute_server->ReleaseSPage(table, m_rid.page_no_);
    data_item_ptr = dtx->SqlVisibleRow(std::move(row));
}

void Scan::next() {
    data_item_ptr.reset();
    if (point_lookup) return;
    while (m_rid.page_no_ < m_fileHdr->num_pages_) {
        auto* data = m_dtx->compute_server->FetchSPage(m_tableID, m_rid.page_no_);
        auto* bitmap = data + sizeof(RmPageHdr);
        m_rid.slot_no_ = Bitmap::next_bit(true, bitmap, m_fileHdr->num_records_per_page_, m_rid.slot_no_);
        if (m_rid.slot_no_ == m_fileHdr->num_records_per_page_) {
            m_dtx->compute_server->ReleaseSPage(m_tableID, m_rid.page_no_);
            ++m_rid.page_no_;
            m_rid.slot_no_ = -1;
            continue;
        }
        auto* tuple = bitmap + m_fileHdr->bitmap_size_ +
            m_rid.slot_no_ * (sizeof(itemkey_t) + m_fileHdr->record_size_);
        memcpy(&m_key, tuple, sizeof(m_key));
        auto row = mvcc::CopyRow(data, *m_fileHdr,
            {uint32_t(m_tableID), uint32_t(m_rid.page_no_), uint32_t(m_rid.slot_no_), m_key});
        m_dtx->compute_server->ReleaseSPage(m_tableID, m_rid.page_no_);
        data_item_ptr = m_dtx->SqlVisibleRow(std::move(row));
        if (data_item_ptr) return;
    }
}

bool Scan::is_end() const { return !data_item_ptr; }
Rid Scan::rid() const { return m_rid; }
DataItemPtr Scan::GetDataItemPtr() const { return data_item_ptr; }
itemkey_t Scan::getKey() const { return m_key; }
