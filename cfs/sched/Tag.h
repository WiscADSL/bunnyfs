#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>

namespace sched {
class Tenant;

// Tag is used to identify a cache access is from a tenant
union Tag {
  // Dummy tenants are not real; they are only created to represent a specific
  // catalog of cache (e.g., unallocated cache space; global cache space)
  enum Dummy : uint64_t { UNALLOC, GLOBAL };

  Tenant* tenant;  // real tenant
  Dummy dummpy;
  uint64_t raw;

  Tenant* get_tenant() const {
    if (dummpy == Dummy::UNALLOC || dummpy == Dummy::GLOBAL) return nullptr;
    return tenant;
  }

  bool operator==(Tag rhs) const { return raw == rhs.raw; }

  friend std::ostream& operator<<(std::ostream& os, const Tag& t) {
    if (t.dummpy == Dummy::UNALLOC) return os << "UNALLOC";
    if (t.dummpy == Dummy::GLOBAL) return os << "GLOBAL";
    return os << t.tenant;
  }
};

namespace tag {
constexpr static Tag unalloc = {.dummpy = Tag::Dummy::UNALLOC};
constexpr static Tag global = {.dummpy = Tag::Dummy::GLOBAL};
}  // namespace tag

static_assert(sizeof(union Tag) == 8, "Tag should be 64-bit");

}  // namespace sched

namespace std {
template <>
struct hash<sched::Tag> {
  std::size_t operator()(const sched::Tag& tag) const {
    return std::hash<uint64_t>{}(tag.raw);
  }
};

};  // namespace std
