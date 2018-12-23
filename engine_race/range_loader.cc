//
// Created by root on 12/5/18.
//

#include <cassert>
#include "range_loader.h"
#include "engine_race.h"
#include "Bucket.h"

namespace polar_race {


  RangeLoader::RangeLoader(EngineRace *db)
  : db_(db), read_index_(BUCKET_NUM, 0), range_count_(0), range_flag_(false) {

    for (int i = 0; i < 3; ++i) {
      char *base = (char *) memalign(4096, MAX_FILE_SIZE);
      buffer_pool_.push_back(base);
    }
    for (int j = 0; j < RANGE_WORKER_NUM; ++j) {
      new std::thread(&RangeLoader::Run, this, j);
    }
  }

  void RangeLoader::AddWatcher() {
    bool flag = false;
    if(range_flag_.compare_exchange_strong(flag, true, std::memory_order_seq_cst)) {
//      range_count_++;
      PushReadTaskToQueue();
    }
    start_cv_.notify_all();
    for (int i = 0; i < BUCKET_NUM; ++i) {
      db_->buckets_[i].ref_count_++;
    }
  }

  void RangeLoader::ReleaseBucket(int id) {
    if (--db_->buckets_[id].ref_count_ == 0) {
//      std::unique_lock<std::mutex> lock(db_->buckets_[id].mutex_);
      std::unique_lock<std::mutex> lock_pool(buffer_pool_mutex_);
      assert(db_->buckets_[id].data_ != nullptr);
      buffer_pool_.push_back(db_->buckets_[id].data_);
      db_->buckets_[id].data_ = nullptr;
      db_->buckets_[id].data_loaded_ = false;
//      log_debug("released bucket data: %d pool size: %d", id, buffer_pool_.size());
      pool_empty_cv_.notify_all();
      if(id == last_not_empty_id_) {
        all_done_cv_.notify_all();
      }
    }
  }

  void RangeLoader::Run(int tid) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(tid, &cpu_set);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
    if(rc != 0) {
      log_debug("set affinity error, %s", strerror(errno));
    }
    int read_flag = 0;
    while (true) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        log_debug("wait range start");
        while(task_queue_.empty()) {
          start_cv_.wait(lock);
        }
      }
      int next_file = 0;
      int next_pos = 0;
      int read_size = 0;
      while (true) {
        {
          std::unique_lock<std::mutex> lock(mutex_);
//          while (next_file < BUCKET_NUM) {
//            if(read_flag < db_->buckets_[next_file].read_flag_) {
//              next_file++;
//              continue;
//            }
//            const uint32_t entry_count = *(db_->buckets_[next_file].entry_count_);
//            const uint32_t bucket_data_size = entry_count * VALUE_SIZE;
//            if (read_index_[next_file] >= bucket_data_size) {
//              next_file++;
//              continue;
//            } else {
//              next_pos = read_index_[next_file];
//              //
//              if (bucket_data_size - next_pos <= READ_PART_SIZE) {
//                read_size = bucket_data_size - next_pos;
//              } else {
//                read_size = READ_PART_SIZE;
//              }
//              read_index_[next_file] += read_size;
//              break;
//            }
//          }

          if(task_queue_.empty()) {
            range_flag_.store(false);
            break;
          } else {
            ReadTask readTask = task_queue_.front();
            task_queue_.pop_front();
            next_file = readTask.bid;
            next_pos = readTask.offset;
            read_size = readTask.len;
//            log_debug("read task: bid: %u pos: %u size: %u", next_file, next_pos, read_size);
          }
        }
//        if (next_file >= BUCKET_NUM) {
//          break;
//        }
        Bucket &bucket = db_->buckets_[next_file];
        if (bucket.data_ == nullptr) {
//          std::unique_lock<std::mutex> lock(bucket.mutex_);
          std::unique_lock<std::mutex> lock_pool(buffer_pool_mutex_);
          while (true) {
            if (bucket.data_ == nullptr && buffer_pool_.empty()) {
              pool_empty_cv_.wait(lock_pool);
            } else {
              break;
            }
          }
//          while (bucket.data_ == nullptr){
//
//            if (buffer_pool_.empty()) {
//              pool_empty_cv_.wait(lock_pool);
//              log_debug("buffer pool size: %d, owns_lock: %d",
//                buffer_pool_.size(), lock_pool.owns_lock());
//            } else {
//              assert(bucket.data_ == nullptr);
          if (bucket.data_ == nullptr) {
            bucket.data_ = buffer_pool_.front();
            buffer_pool_.pop_front();
//            log_debug("get a buffer pool size: %d owns_lock:%d bucket: %d",
//                      buffer_pool_.size(), lock_pool.owns_lock(), next_file);
          }
//            }
        }
//      }

        bucket.LoadFile(bucket.data_ + next_pos, next_pos, read_size);
        if (--bucket.read_part_remain_ == 0) {
//          bucket.read_part_remain_ = RANGE_READ_PART;
//          read_index_[next_file] = 0;
          bucket.data_loaded_ = true;
          ++bucket.read_flag_;
//          log_debug("bucket data ok : %d", next_file);
          next_file++;

          bucket.data_ready_cv_.notify_all();
        }
      }
      ++read_flag;
      if (next_file < 0) {
        break;
      }
    }
  }

  void RangeLoader::PushReadTaskToQueue() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (int bid = 0; bid < BUCKET_NUM; ++bid) {
      const uint32_t entry_count = *(db_->buckets_[bid].entry_count_);
      const uint32_t bucket_data_size = entry_count * VALUE_SIZE;
      uint32_t next_pos = 0;
      uint32_t read_size = 0;
      int count = 0;
      while(next_pos < bucket_data_size) {
        last_not_empty_id_ = bid;
        if (bucket_data_size - next_pos < READ_PART_SIZE) {
          read_size = bucket_data_size - next_pos;
        } else {
          read_size = READ_PART_SIZE;
        }
        task_queue_.emplace_back(bid, next_pos, read_size);
        next_pos += read_size;
        ++count;
      }
      db_->buckets_[bid].read_part_remain_ = count;
    }
  }

  void RangeLoader::WaitAllDone() {
    std::unique_lock<std::mutex> lock(mutex_);
    while(db_->buckets_[last_not_empty_id_].ref_count_.load() != 0) {
      all_done_cv_.wait(lock);
    }
  }
}
