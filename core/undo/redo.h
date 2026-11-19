#pragma once
#include "undo/format.h"
#include "storage/log_record.h"

namespace undo {
enum class Operation : uint32_t { Initialize = 1, Append = 2, Seal = 3 };
class Redo final : public LogRecord {
public:
    Redo();
    Redo(Operation operation, PageKey page, mvcc::TxnId segment, uint64_t previous, uint32_t slot = 0,
         Record record = {});
    void serialize(char* dest) const override;
    void deserialize(const char* src) override;
    void Decode(const char* src, size_t size);
    // Returns the page after applying one redo; duplicate content is checked.
    void Apply(PageBytes& bytes, bool exists) const;
    Operation operation = Operation::Initialize;
    PageKey page;
    mvcc::TxnId segment;
    uint32_t slot = 0;
    Record record;

private:
    std::string Payload() const;
};
}  // namespace undo
