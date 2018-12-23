//
// Created by root on 11/13/18.
//

#include <malloc.h>
#include <atomic>
#include "Bucket.h"
#include "engine_race.h"
#include "util.h"
#include <algorithm>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "likely.h"
#include "log.h"
namespace polar_race {


  void Bucket::Init(EngineRace* db, uint32_t id, bool is_new, int key_fd, int value_fd) {
    this->db_ = db;
    this->id_ = id;
    this->is_new_ = is_new;
    this->data_ = nullptr;

    key_data_fd_ = key_fd;
    value_data_fd_ = value_fd;
    entry_count_ = db->mappedBufferManager.GetSizePtr(id_);
//    assert(*entry_count_ == id_);
    key_buffer_ = db->mappedBufferManager.GetKeyBuffer(id_);
    value_buffer_ = db->mappedBufferManager.GetValueBuffer(id_);
//    log_debug("bucket %4d created", id_);
    index_.reserve(*entry_count_);
//    read_part_remain_ = ((*entry_count_ - *entry_count_ % VALUE_BUFFER_ITEM_COUNT) * VALUE_SIZE) / READ_PART_SIZE + 1;
  }


  void Bucket::Write(const char *key, const char *value) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint32_t count = *entry_count_;
    uint32_t value_buffer_offset = (count % VALUE_BUFFER_ITEM_COUNT) * VALUE_SIZE;
    memcpy(value_buffer_ + value_buffer_offset, value, VALUE_SIZE);

    uint32_t key_buffer_offset = (count % KEY_BUFFER_ITEM_COUNT) * KEY_SIZE;
    memcpy(key_buffer_ + key_buffer_offset, key, KEY_SIZE);
//
    if ((count + 1) % VALUE_BUFFER_ITEM_COUNT == 0) {
      uint64_t offset = (count + 1LU) * VALUE_SIZE - VALUE_BUFFER_SIZE;
      assert(offset < BUCKET_VALUE_MAX_SIZE);
      uint64_t offset_gourp = GetGroupValueFileOffset(id_, offset);
      assert(offset_gourp % 4096 == 0);
      assert(long(value_buffer_) % 4096 == 0);
//      log_debug("fd: %u offset: %lu goffset: %lu", id_, offset, offset_gourp);
      ssize_t ret = pwrite64(value_data_fd_, value_buffer_, VALUE_BUFFER_SIZE, offset_gourp);
      if (ret != VALUE_BUFFER_SIZE) {
        log_debug("write file offset: %lu error: %s ", offset, strerror(errno));
        exit(-1);
      }
    }
//
    if ((count + 1) % KEY_BUFFER_ITEM_COUNT == 0) {
      uint64_t offset = (count + 1LU) * KEY_SIZE - KEY_BUFFER_SIZE;
      uint64_t offset_gourp = GetGroupKeyFileOffset(id_, offset);
      assert(offset % 4096 == 0);
      ssize_t ret = pwrite64(key_data_fd_, key_buffer_, KEY_BUFFER_SIZE, offset_gourp);
      if (ret != KEY_BUFFER_SIZE) {
        log_debug("write file offset: %lu error: %s", offset, strerror(errno));
      }
    }
////    log_debug("bucket %2d write %s pos: %5d %s", id_, hexdump(key, 8).c_str(), count,
////      hexdump(value, 16).c_str());
    ++(*entry_count_);
//    if(write(value_data_write_fd_, value, VALUE_SIZE) != VALUE_SIZE) {
//      log_debug("write file error: %s", strerror(errno));
//      exit(-1);
//    }
//    if(write(key_data_write_fd_, key, KEY_SIZE) != KEY_SIZE) {
//      log_debug("write file error: %s", strerror(errno));
//      exit(-1);
//    }
//    return kSucc;
  }
  std::atomic<long> ops;
  RetCode Bucket::Read(const char *key, std::string *value) {
//    CheckLoaded();
    uint64_t key_num = to_uint64(key);
    assert(key_num >> (64 - BUCKET_NUM_BITS) == id_);
    Index index1(key_num, UINT32_MAX);
    const auto it = std::lower_bound(index_.begin(), index_.end(), index1);
    if (it == index_.end() || it->key != key_num) {
//      log_debug("key %s not found", hexdump(key, 8).c_str());
      return kNotFound;
    } else {
//      value->clear();
//      value->reserve(VALUE_SIZE);
      uint64_t pos;
      char localBuf[VALUE_SIZE] __attribute__((aligned(512)));
      if (it->block_index / VALUE_BUFFER_ITEM_COUNT >= *entry_count_ / VALUE_BUFFER_ITEM_COUNT) {
//        pos = (it->block_index - (*entry_count_ / VALUE_BUFFER_ITEM_COUNT)
//          * VALUE_BUFFER_ITEM_COUNT) * VALUE_SIZE;

        pos = (it->block_index % VALUE_BUFFER_ITEM_COUNT) * VALUE_SIZE;
//        log_debug("pos = %u", pos);
        value->assign(value_buffer_ + pos, VALUE_SIZE);
      } else {

        pos = ((uint64_t) it->block_index) * VALUE_SIZE;
        pos = GetGroupValueFileOffset(id_, pos);
        ssize_t ret = pread64(value_data_fd_, localBuf, VALUE_SIZE, pos);
        if (ret != VALUE_SIZE) {
          log_debug("read error: %s", strerror(errno));
        }
        value->assign(localBuf, VALUE_SIZE);

//        log_debug("bucket %d, read %s pos: %5ld value:%s", id_, hexdump(key, 8).c_str(),
//          pos / VALUE_SIZE, hexdump(localBuf, 16).c_str());
      }
//      log_debug("bucket:%2d key: %s value : %s", id_,
//        hexdump(key, 8).c_str(), hexdump(value->data(), 16).c_str());
//      if(hexdump(key, 8) == "0100000004656E64"){
//        hexdump(key, 8);
//      }
//      if(++ops < 100) {
//        assert(value->size() == VALUE_SIZE);
//        log_debug("read %s pos: %6ld, value: %s", hexdump(key, 8).c_str(), pos,
//                  hexdump(value->data(), VALUE_SIZE).c_str());
//      }
      return kSucc;
    }
  }

  int index_cmp(const void *t1, const void *t2) {
    Index *index1 = (Index *) t1;
    Index *index2 = (Index *) t2;
    if (index1->key == index2->key) return index1->block_index < index2->block_index;
    else return index1->key > index2->key;
  }

  void Bucket::LoadIndex(char *buf) {
    if (is_new_) {
      return;
    }
    const uint32_t entry_count = *(entry_count_);
    if(entry_count == 0) {
      return;
    }
//    std::string keyDataFileName = dir_ + "/key" + std::to_string(id_) + ".data";
//    uint32_t v = *entry_count_;


    uint32_t entry_index = 0;
    const uint64_t entry_count_file = entry_count - (entry_count % KEY_BUFFER_ITEM_COUNT);
    uint64_t offset = GetGroupKeyFileOffset(id_, 0);
    ssize_t ret = pread64(key_data_fd_, buf, entry_count_file * KEY_SIZE, offset);
    if (ret != entry_count_file * KEY_SIZE) {
      log_debug("read error, ret: %ld, arg: %lu, error: %s", ret, entry_count_file * KEY_SIZE, strerror(errno));
      exit(-1);
    }
    for (uint64_t j = 0; j < entry_count_file; ++j) {
      uint64_t key = to_uint64(buf + j * KEY_SIZE);
      index_.emplace_back(key, entry_index);
      ++entry_index;
    }

    for (int i = 0; i < (entry_count % KEY_BUFFER_ITEM_COUNT); ++i) {
      uint64_t key = to_uint64(key_buffer_ + i * KEY_SIZE);
      index_.emplace_back(key, entry_index);
      ++entry_index;
    }
//    std::qsort(index_.data(), index_.size(), sizeof(Index), &index_cmp);
    std::sort(index_.begin(), index_.end());

    index_loaded_ = true;
    assert(entry_index == *entry_count_);
//    log_debug("%2d load index, size: %lu", id_, index_.size());
//    return entry_count;
  }

  uint32_t Bucket::GetEntryCount() {
    return *entry_count_;
  }

  void Bucket::LoadFile(char* buf, uint64_t offset, uint64_t len) {
    const uint64_t file_size = (*entry_count_ - *entry_count_%VALUE_BUFFER_ITEM_COUNT)*VALUE_SIZE;
    const uint64_t remain = std::min(file_size - offset, len);
//    log_debug("id: %u offset: %lu goffset: %lu len: %lu",
//      id_, offset, GetGroupValueFileOffset(id_, offset), len);
    ssize_t ret = pread64(value_data_fd_, buf, remain, GetGroupValueFileOffset(id_, offset));
    if(ret != remain) {
      log_debug("read error, offset: %lu len: %lu ret: %ld %s", offset, len, ret, strerror(errno));
      exit(-1);
    }
    if(remain < len) {
      memcpy(buf+remain, value_buffer_, *entry_count_%VALUE_BUFFER_ITEM_COUNT*VALUE_SIZE);
    }
  }

  char *Bucket::GetData() {
    std::unique_lock<std::mutex> lock(mutex_);
    while(!data_loaded_) {
      data_ready_cv_.wait(lock);
    }
//    log_debug("range get data : %d", id_);
    assert(data_ != nullptr);
    return data_;
  }
}