#include "primer/count_min_sketch.h"

#include <algorithm>
#include <stdexcept>
#include <limits>

namespace bustub {

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth)
    : width_(width), depth_(depth), counts_(depth, std::vector<uint32_t>(width, 0)), row_locks_(depth) {
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_), depth_(other.depth_), counts_(std::move(other.counts_)) {
  row_locks_.resize(depth_);
  other.width_ = 0;
  other.depth_ = 0;
  other.counts_.clear();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    counts_ = std::move(other.counts_);
    row_locks_.clear();
    row_locks_.resize(depth_);
    other.width_ = 0;
    other.depth_ = 0;
    other.counts_.clear();
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for (size_t r = 0; r < depth_; r++) {
    size_t col = hash_functions_[r](item);
    std::lock_guard<std::mutex> lock(row_locks_[r]);
    counts_[r][col]++;
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();
  for (size_t r = 0; r < depth_; r++) {
    size_t col = hash_functions_[r](item);
    std::lock_guard<std::mutex> lock(row_locks_[r]);
    min_count = std::min(min_count, counts_[r][col]);
  }
  return (min_count == std::numeric_limits<uint32_t>::max()) ? 0 : min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  for (size_t r = 0; r < depth_; r++) {
    std::lock_guard<std::mutex> lock(row_locks_[r]);
    std::fill(counts_[r].begin(), counts_[r].end(), 0);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  for (size_t r = 0; r < depth_; r++) {
    std::lock_guard<std::mutex> lock(row_locks_[r]);
    for (size_t c = 0; c < width_; c++) {
      counts_[r][c] += other.counts_[r][c];
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> all;
  all.reserve(candidates.size());

  for (const auto &item : candidates) {
    all.emplace_back(item, Count(item));
  }

  if (k >= all.size()) {
    std::sort(all.begin(), all.end(), [](auto &a, auto &b) { return a.second > b.second; });
    return all;
  }

  std::nth_element(all.begin(), all.begin() + k, all.end(),
                   [](auto &a, auto &b) { return a.second > b.second; });

  std::vector<std::pair<KeyType, uint32_t>> top(all.begin(), all.begin() + k);
  std::sort(top.begin(), top.end(), [](auto &a, auto &b) { return a.second > b.second; });

  return top;
}

// Explicit instantiations for tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;
template class CountMinSketch<int>;

}  // namespace bustub
