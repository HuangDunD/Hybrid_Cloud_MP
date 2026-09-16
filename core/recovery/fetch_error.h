#pragma once
#include <stdexcept>

namespace recovery {
class PageUnavailable : public std::runtime_error {
public:
    explicit PageUnavailable(const char* message) : std::runtime_error(message) {}
};
class RequestCancelled : public std::runtime_error {
public:
    RequestCancelled() : std::runtime_error("request cancelled or deadline expired") {}
};
}  // namespace recovery
