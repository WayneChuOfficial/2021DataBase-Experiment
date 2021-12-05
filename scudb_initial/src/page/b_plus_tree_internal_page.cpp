/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"
namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);                            
  SetPageId(page_id);
  SetParentPageId(parent_id);
  //the first key always remains invalid
  int size = (PAGE_SIZE - sizeof(B_PLUS_TREE_INTERNAL_PAGE_TYPE)) /  (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(0 <= index && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i = 0; i < GetSize(); i++) {
    if (array[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  assert(0 <= index && index < GetSize());
  return array[index].second; 
  }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value)
{
  assert(0 <= index && index < GetSize());
  array[index].second = value;
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const 
{
  assert(GetSize() > 1);
  int foundIndex = 0;
  int e = GetSize();
  for (int b = 1; b < e; b++) {
    if (comparator(array[b].first, key) <= 0) 
    {
      foundIndex = b;
    }
  }
  return array[foundIndex].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
    // must be a new page
    assert(GetSize() == 1);
    array[0].second = old_value;
    array[1].first = new_key;
    array[1].second = new_value;
    IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int preIndex = ValueIndex(old_value);
  assert(preIndex != -1);
  
  // Shift all nodes from preIndex + 1 to right by 1
  for (int i = GetSize(); i >= preIndex + 1; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }

  // insert node after previous old node
  array[preIndex + 1].first = new_key;
  array[preIndex + 1].second = new_value;

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
      // remove from split index to the end
  int splitIndex = (GetSize() + 1) / 2;
  recipient->CopyHalfFrom(&array[GetSize() - splitIndex],splitIndex, buffer_pool_manager);

  // change the parent of the removed pages
  for (auto index = GetSize() - splitIndex; index < GetSize(); ++index)
  {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  IncreaseSize(-1 * splitIndex);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
      for (int i = 0; i < size; i++) {
        //skip the first key
      array[i] = items[i];
    }
    IncreaseSize(size-1);
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(0 <= index && index < GetSize());
  for (int i = index; i < GetSize() - 1; i++) {
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  assert(GetSize() == 1);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  // get parent page
  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parentNode =
        reinterpret_cast<BPlusTreeInternalPage *>(pPage->GetData());

  SetKeyAt(0, parentNode->KeyAt(index_in_parent));
  //assert(parentNode->ValueAt(index_in_parent) == GetPageId());

  // unpin parent page
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  recipient->CopyAllFrom(&array[0], GetSize(), buffer_pool_manager);  

  // change the parent of the moved pages
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i), true);
  }
  //set empty
  SetSize(0); 
  }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int startIndex = GetSize();
    for (int i = 0; i < size; i++) 
    {//skip the first key
      array[startIndex + i] = items[i];
    }    
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  MappingType pair{KeyAt(1),ValueAt(0)};
  //SetValueAt(0,ValueAt(1));
  array[0].second = ValueAt(1);
  Remove(1);
  recipient->CopyLastFrom(pair, buffer_pool_manager);

  // set the transfering node's child page's parent to recipient 
  auto *page = buffer_pool_manager->FetchPage(array[0].second);
  BPlusTreePage *node =
      reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(array[0].second, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parentNode =
        reinterpret_cast<BPlusTreeInternalPage *>(pPage->GetData());
  int ourPageIdInParentIndex = parentNode->ValueIndex(GetPageId());
  auto Key = parentNode->KeyAt(ourPageIdInParentIndex + 1);
  //array[GetSize()] = {Key,pair.second};
  array[GetSize()].first = Key;
  array[GetSize()].second = pair.second;
  IncreaseSize(1);
  parentNode->SetKeyAt(ourPageIdInParentIndex + 1, pair.first); 
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  
  MappingType pair = array[GetSize() - 1];
  recipient->CopyFirstFrom(pair,parent_index, buffer_pool_manager);

  // Set the moved node's child to the correct parent
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node =
      reinterpret_cast<BPlusTreePage *>(page->GetData());
  //assert(node);
  node->SetParentPageId(recipient->GetPageId());
  
  buffer_pool_manager->UnpinPage(pair.second, true); 
  Remove(GetSize() - 1); // actually this doesn't matter , so we need to increasesize(-1)
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  
  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parentNode =
        reinterpret_cast<BPlusTreeInternalPage *>(pPage->GetData());
  auto key = parentNode->KeyAt(parent_index);
  parentNode->SetKeyAt(parent_index,pair.first);
  
  // move every item to the next, to give space to our new record
  for (int i = GetSize(); i > 0; i--) 
  {
    array[i] = array[i - 1];
  }
  array[1].first = key;
  array[0].second = pair.second;
  IncreaseSize(1);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true); 
}

INDEX_TEMPLATE_ARGUMENTS
MappingType B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushUpIndex() {
  MappingType pair = array[1];
  array[0].second = pair.second;

  Remove(1);

  return pair;
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
