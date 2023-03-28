//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetLSN();
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(size_t idx) -> const std::pair<KeyType, ValueType> & { return array_[idx]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> size_t {
  auto idx = std::lower_bound(
      array_, array_ + GetSize(), key,
      [&comparator](const MappingType &pair, const KeyType &key) { return comparator(pair.first, key) < 0; });
  return idx - array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> size_t {
  int idx = KeyIndex(key, comparator);
  if (idx == GetSize()) {
    array_[idx] = {key, value};
    IncreaseSize(1);
    return GetSize();
  }
  if (comparator(array_[idx].first, key) == 0) {
    return GetSize();
  }
  std::move_backward(array_ + idx, array_ + GetSize(), array_ + GetSize() + 1);
  array_[idx] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, ValueType *value, const KeyComparator &comparator) -> bool {
  int idx = KeyIndex(key, comparator);
  if (idx == GetSize()) {
    return false;
  }
  if (!comparator(array_[idx].first, key)) {
    return false;
  }
  *value = array_[idx].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(const std::pair<KeyType, ValueType> *from, size_t size) {
  std::copy(from, from + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(bustub::BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *to) {
  size_t begin_size = GetMinSize();
  size_t end_size = GetMaxSize();
  to->CopyNFrom(array_ + begin_size,end_size - begin_size);
  SetSize(begin_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  int idx = KeyIndex(key, comparator);
  if (idx == GetSize() || !comparator(array_[idx].first, key)) {
    return GetSize();
  }
  std::move(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
  return GetSize();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
