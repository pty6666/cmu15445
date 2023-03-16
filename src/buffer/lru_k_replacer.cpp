//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_{k} {}

auto LRUKReplacer::Judge(bustub::frame_id_t f1, bustub::frame_id_t f2) -> frame_id_t {
  auto temp1 = mp_.find(f1);
  auto temp2 = mp_.find(f2);
  if (temp1->second.list_.size() < k_ && temp2->second.list_.size() == k_) {
    return f1;
  }
  if (temp1->second.list_.size() == k_ && temp2->second.list_.size() < k_) {
    return f2;
  }
  return temp1->second.list_.front() < temp2->second.list_.front() ? f1 : f2;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  *frame_id = -1;
  for (const auto &i : mp_) {
    if (i.second.evictable_) {
      if (*frame_id == -1) {
        *frame_id = i.first;
      } else {
        *frame_id = Judge(*frame_id, i.first);
      }
    }
  }
  if (*frame_id == -1) {
    return false;
  }
  curr_size_--;
  mp_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto temp = mp_.find(frame_id);
  if (temp == mp_.end() && mp_.size() == replacer_size_) {
    return;
  }
  if (temp == mp_.end()) {
    mp_[frame_id];
  }
  temp = mp_.find(frame_id);
  if (temp->second.list_.size() == k_) {
    temp->second.list_.pop_front();
  }
  temp->second.list_.emplace_back(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto temp = mp_.find(frame_id);
  if (temp == mp_.end()) {
    return;
  }

  if (!temp->second.evictable_ && set_evictable) {
    curr_size_++;
  } else if (temp->second.evictable_ && !set_evictable) {
    curr_size_--;
  }
  temp->second.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto temp = mp_.find(frame_id);
  if(temp == mp_.end()){
    return;
  }
  if(!temp->second.evictable_) {
    return;
  }
  mp_.erase(temp);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
}  // namespace bustub
