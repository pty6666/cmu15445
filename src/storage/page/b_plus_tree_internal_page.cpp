//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto idx = std::lower_bound(
      array_ + 1, array_ + GetSize(), key,
      [&comparator](const std::pair<KeyType, ValueType> &pair, KeyType k) { return comparator(pair.first, k) < 0; });
  if (idx == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(idx->first, key) == 0) {
    return idx->second;
  }
  return std::prev(idx)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &key,
                                                     const ValueType &new_value) {
  SetKeyAt(1, key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  auto new_value_idx = ValueIndex(old_value) + 1;
  std::move_backward(array_ + new_value_idx, array_ + GetSize(), array_ + GetSize() + 1);
  array_[new_value_idx] = {new_key, new_value};
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *to,
                                                bustub::BufferPoolManager *buffer_pool) {
  int begin_idx = GetMinSize();
  int end_idx = GetSize();
  SetSize(begin_idx);
  to->CopyNFrom(array_ + begin_idx, end_idx - begin_idx, buffer_pool);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(const std::pair<KeyType, ValueType> *from, size_t size,
                                               bustub::BufferPoolManager *buffer_pool) {
  std::copy(from, from + size, array_ + GetSize());
  for (size_t i = 0; i < size; i++) {
    auto page = buffer_pool->FetchPage((from + i)->second);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool->UnpinPage(node->GetPageId(), true);
  }
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int idx) {
  std::move(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  auto ret = ValueAt(0);
  SetSize(0);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *to,
                                               const KeyType &mid_key, bustub::BufferPoolManager *buffer_pool) {
  SetKeyAt(0, mid_key);
  to->CopyNFrom(array_, GetSize(), buffer_pool);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *to,
                                                      const KeyType &mid_key, bustub::BufferPoolManager *buffer_pool) {
  SetKeyAt(0, mid_key);
  to->CopyLastFrom(array_[0], buffer_pool);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *to,
                                                       const KeyType &mid_key, bustub::BufferPoolManager *buffer_pool) {
  to->SetKeyAt(0, mid_key);
  to->CopyFirstFrom(*(array_ + GetSize() - 1), buffer_pool);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const std::pair<KeyType, ValueType> &from,
                                                  bustub::BufferPoolManager *buffer_pool) {
  *(array_ + GetSize()) = from;
  IncreaseSize(1);
  auto page = buffer_pool->FetchPage(from.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool->UnpinPage(node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &from,
                                                   bustub::BufferPoolManager *buffer_pool) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  IncreaseSize(1);
  array_[0] = from;
  auto page = buffer_pool->FetchPage(from.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool->UnpinPage(node->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
