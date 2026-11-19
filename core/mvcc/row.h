#pragma once
#include "base/data_item.h"
#include "record/record.h"
#include "undo/format.h"
namespace mvcc {
// Caller holds the main page S/X latch. Only the value-copy escapes that latch.
undo::RowImage CopyRow(const char* data, const RmFileHdr& file, const undo::RowId& id);
// Rebuild the logical row from a record-sized RPC buffer (DataItem + value).
undo::RowImage RowImageFromItem(const DataItem& item, const undo::RowId& id);
DataItem* RowAt(char* data, const RmFileHdr& file, const undo::RowId& id);
void InstallRow(DataItem& disk, const undo::RowImage& row);  // preserves all lock metadata
void CopyToItem(DataItem& item, const undo::RowImage& row);
}  // namespace mvcc
