//
// Created by root on 12/4/18.
//

#include "mapped_buffer_manager.h"

namespace polar_race {
  MappedBufferManager::MappedBufferManager(const std::string &filename, bool is_new)
    : filename_(filename), fd_(-1), is_new_(is_new),
      map_base_(0), key_buffer_base_(0), value_buffer_base_(0), size_base_(0) {

  }

  void MappedBufferManager::Init() {
    fd_ = open(filename_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
      log_debug("open file error: %s", strerror(errno));
      exit(-1);
    }
    const int file_size = (VALUE_BUFFER_SIZE + KEY_BUFFER_SIZE + 64) * BUCKET_NUM;
    int ret = posix_fallocate(fd_, 0, file_size);
    if (ret != 0) {
      log_debug("allocate file error: %s", strerror(errno));
      exit(-1);
    }
    map_base_ = (char *) mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (map_base_ == MAP_FAILED) {
      log_debug("mmap error: %s", strerror(errno));
      exit(-1);
    }
    int ret1 = 0;

    key_buffer_base_ = map_base_;
    value_buffer_base_ = map_base_ + KEY_BUFFER_SIZE * BUCKET_NUM;
    size_base_ = (uint32_t *) (map_base_ + (KEY_BUFFER_SIZE + VALUE_BUFFER_SIZE) * BUCKET_NUM);

    for (uint32_t i = 0; i < BUCKET_NUM; ++i) {
//      *(size_base_+i) = i;
//      += *(map_base_+i);
    }
  }

  char *MappedBufferManager::GetKeyBuffer(int id) {
    if (id >= BUCKET_NUM) {
      log_debug("arg error BUCKET_NUM: %d, id: %d", BUCKET_NUM, id);
      exit(-1);
    }
    return key_buffer_base_ + KEY_BUFFER_SIZE * id;
  }

  char *MappedBufferManager::GetValueBuffer(int id) {
    if (id >= BUCKET_NUM) {
      log_debug("arg error BUCKET_NUM: %d, id: %d", BUCKET_NUM, id);
      exit(-1);
    }
    return value_buffer_base_ + VALUE_BUFFER_SIZE * id;
  }

  uint32_t *MappedBufferManager::GetSizePtr(int id) {
    if (id >= BUCKET_NUM) {
      log_debug("arg error BUCKET_NUM: %d, id: %d", BUCKET_NUM, id);
      exit(-1);
    }
    return size_base_ + id;
  }
}