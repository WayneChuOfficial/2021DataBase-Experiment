/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "common/logger.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);                            
  SetPageId(page_id);
  SetParentPageId(parent_id);

  SetNextPageId(INVALID_PAGE_ID);
  
  // header size is 24 bytes
  // Total record size divded by each record size is max allowed size
  // IMPORTANT: leave a always available slot for insertion! Otherwise, insert will cause memory stomp
  int size = (PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE)) / (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); i++) 
  {
    if (comparator(array[i].first, key) >= 0) 
    {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  MappingType pair;
  pair.first = key;
  pair.second = value;

  int upperBoundKeyIndex = KeyIndex(key, comparator);//upperBoundKeyIndex >= key
  if (upperBoundKeyIndex == -1) {// all index are smaller than key, key should be inserted to last
    array[GetSize()] = pair;
  } else {
    //shift and insert
    for (int i = GetSize() - 1; i >= upperBoundKeyIndex; i--) {
      array[i + 1].first = array[i].first;
      array[i + 1].second = array[i].second;
    }
    array[upperBoundKeyIndex].first = pair.first;
    array[upperBoundKeyIndex].second = pair.second;
  }
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  // remove from split index to the end
  int splitIndex = GetSize() / 2;
  recipient->CopyHalfFrom(&array[GetSize() - splitIndex], splitIndex);

  IncreaseSize(-1 *  splitIndex);
  recipient->SetNextPageId(GetNextPageId());
  //SetNextPageId(recipient->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  //new page
  for (int i = 0; i < size; i++) {
    array[i] = items[i];
    //array[i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  for(int i = 0; i < GetSize(); i++) 
  {
    int result = comparator(array[i].first,key);
    if (result == 0) 
    {
      value = array[i].second;
      return true;
    }
  }                                          
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  int keyIndex = -1;
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) == 0) {
      keyIndex = i;
      break;
    }
  }

  if (keyIndex == -1) {
    //does not exist
    return GetSize(); 
  }

  // shift and delete
  for (int i = keyIndex; i < GetSize()-1; ++i) {
    array[i].first = array[i+1].first;
    array[i].second = array[i+1].second;
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  recipient->CopyAllFrom(&array[0], GetSize());
  // leaf has no children
  
  // assumption: current page is at the right hand of recipient
  recipient->SetNextPageId(GetNextPageId());

  SetSize(0); // set this page empty
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  int startIndex = GetSize();
  for (int i = 0; i < size; i++) {
    array[startIndex + i].first = items[i].first;
    array[startIndex + i].second = items[i].second;
  }    
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

  recipient->CopyLastFrom(array[0]);
  // remove index 0
  for (int i = 0; i < GetSize(); ++i) 
  {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }

  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  if (pPage == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while MoveFirstToEndOf");
  }
  auto *parentNode =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pPage->GetData());

  // change key in the parent page
  int ourPageIdInParentIndex = parentNode->ValueIndex(GetPageId());
  parentNode->SetKeyAt(ourPageIdInParentIndex, KeyAt(0)); 

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
  MappingType pair = array[GetSize() - 1];
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  IncreaseSize(1);
  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parentNode =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pPage->GetData());

  // parentIndex should recipient's position in parent node
  // update the key content
  parentNode->SetKeyAt(parentIndex, item.first); 
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace scudb
