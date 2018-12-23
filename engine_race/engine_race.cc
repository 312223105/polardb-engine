// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <cstdlib>
#include <memory.h>
#include <memory>
#include <thread>
#include "log.h"
#include "util.h"
#include "likely.h"
#include "read_seq_test.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/sysinfo.h>
#include <malloc.h>
#include <cassert>

namespace polar_race {

  void self_log_LockFn(void *udata, int lock) {
    static std::mutex log_mutex;
    if (lock) {
      log_mutex.lock();
    } else {
      log_mutex.unlock();
    }

  }

//#define PRINT_WRITE_VALUE_DATA
//#define PRINT_READ_VALUE_DATA
  RetCode Engine::Open(const std::string &name, Engine **eptr) {
    return EngineRace::Open(name, eptr);
  }

  Engine::~Engine() {
  }
uint64_t start_time = 0;
  RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
    log_debug("start time: %lu", NowMicros());
//    log_set_lock(&self_log_LockFn);
//    log_debug("Open engine, dir : %s", name.c_str());
    *eptr = NULL;
    bool is_new = true;
    if (file_exist((name + "/mapped.data").c_str())) {
      is_new = false;
    }
    start_time = NowMicros();
    EngineRace *engine_race = new EngineRace(name, is_new);
    log_error("[%10lu], start load index, mem usage : %d kb", NowMicros()-start_time, get_memory_usage());
    engine_race->Init();
    log_error("[%10lu], after load index, mem usage : %d kb", NowMicros()-start_time, get_memory_usage());
//    FILE* log_file = fopen("/root/engine_log_first.log", "a+");
//    log_set_fp(log_file);
    *eptr = engine_race;
    return kSucc;
  }

  EngineRace::EngineRace(const std::string &dir, bool is_new)
    : dir_(dir), is_new_(is_new), first_(true), mappedBufferManager(dir + "/mapped.data", is_new),
    rangeLoader_(this), tid_num_(0) {

//    std::thread *dstat = new std::thread([]() {
//      GetStdoutFromCommand("dstat", "dstat -cdilmryt");
//    });
//    dstat = new std::thread([]() {
//      GetStdoutFromCommand("iostat", "iostat -xm 1 nvme0n1 sda");
//    });
    if(is_new) {
      mkdir(dir.c_str(), 0755);
    }


  }

  EngineRace::~EngineRace() {
    log_debug("end time: %lu", NowMicros());
    log_debug("[%10lu] engine_closed", NowMicros()-start_time);
  }


  void EngineRace::PrintFirstOpTimestamp() {
    if(UNLIKELY(first_)) {
      std::unique_lock<std::mutex> lock(mutex_);
      if(first_) {
        first_ = false;
        log_debug("first op time: %lu", NowMicros());
      }
    }
  }
  RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
    PrintFirstOpTimestamp();
    uint64_t key_num = to_uint64(key.data());
    uint32_t seed = (uint32_t) (key_num >> (64 - BUCKET_NUM_BITS));
//    buckets_[seed]->Write(key.data(), value.data());
    buckets_[seed].Write(key.data(), value.data());
//    }
    return kSucc;
  }

  RetCode EngineRace::Read(const PolarString &key, std::string *value) {
    PrintFirstOpTimestamp();
//    static thread_local std::atomic<int> ops(0);
//    static thread_local int tid = tid_num_++;
    uint64_t key_num = to_uint64(key.data());
    uint32_t seed = (uint32_t) (key_num >> (64 - BUCKET_NUM_BITS));
    value->clear();
    RetCode ret = buckets_[seed].Read(key.data(), value);
//    if (value->size() != 4096 ||
//        memcmp(key.data(), value->data(), 8) != 0 ) {
//      log_error("read key %s error", hexdump(key.data(), 8).c_str());
//    }
//    if(++ops == 1000000) {
//      log_debug("read thread %d finished", tid);
//    }
    return ret;
  }

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
//static std::atomic<int> tid(0);
  RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                            Visitor &visitor) {
    PrintFirstOpTimestamp();
//    static thread_local int tid_ = tid++;
//    log_error("range begin, mem usage : %d kb", get_memory_usage());
    if (lower.size() == 0 && upper.size() == 0) {
      rangeLoader_.AddWatcher();
//      uint64_t total_num = 0;
//      for (int i = 0; i < BUCKET_NUM; ++i) {
//        total_num += *buckets_[i].entry_count_;
//      }
//      if(total_num > 2000000) {
//        log_debug("range exit");
//        return kSucc;
//      }
      std::string value(VALUE_SIZE, 0);
//      uint32_t count = 0;
      uint64_t last_key = UINT64_MAX;
//      char *localBuf = (char *) memalign(4096, VALUE_SIZE);
      for (int bid = 0; bid < BUCKET_NUM; ++bid) {
        const char* data;
        if(buckets_[bid].GetEntryCount() != 0) {
//          if(tid_ == 0) {
//            log_debug("tid: %d begin get data %d", tid_, bid);
//          }
          data = buckets_[bid].GetData();
//          FILE* f = fopen("test.dat", "wb");
//          fwrite(data, *buckets_[bid].entry_count_*VALUE_SIZE, 1, f);
//          fclose(f);
        } else {
          continue;
        }
        auto it = buckets_[bid].index_.begin();
        while (true) {
          if (it == buckets_[bid].index_.end()) {
            break;
          }
//          count++;
//          if (count++ > 6300000) {
//            log_debug("range exit");
//            exit(-1);
//          }
          uint64_t key = it->key;

          if (last_key == key) {
            ++it;
            continue;
          }
          last_key = key;
          key = __bswap_64(key);
//          uint64_t pos;
//          if (it->block_index / VALUE_BUFFER_ITEM_COUNT >=
//              *buckets_[i].entry_count_ / VALUE_BUFFER_ITEM_COUNT) {
//            pos = (it->block_index % VALUE_BUFFER_ITEM_COUNT) * VALUE_SIZE;
//            visitor.Visit(PolarString((const char *) &key, 8),
//                          PolarString(buckets_[i].value_buffer_ + pos, VALUE_SIZE));
//          } else {
//            pos = ((uint64_t) it->block_index) * VALUE_SIZE;
//            pread64(buckets_[i].value_data_fd_, localBuf, VALUE_SIZE, pos);
//            visitor.Visit(PolarString((const char *) &key, 8),
//                          PolarString(localBuf, VALUE_SIZE));
//
//          }
          uint64_t pos = ((uint64_t) it->block_index) * VALUE_SIZE;
          visitor.Visit(PolarString((const char*)&key, 8), PolarString(data+pos, VALUE_SIZE));
          ++it;
        }

        rangeLoader_.ReleaseBucket(bid);
      }
    }
//    usleep(200*1000);
    return kSucc;
  }

  void EngineRace::Init() {
    OpenFiles();
    mappedBufferManager.Init();
    for (uint32_t bid = 0; bid < BUCKET_NUM; ++bid) {
      uint32_t gid = GetGroupId(bid);
      buckets_[bid].Init(this, bid, is_new_, key_fd_arr_[gid], value_fd_arr_[gid]);
    }
    std::vector<std::thread> threads;
    const int cpu_num = get_nprocs();
//    const int cpu_num = 1;
    std::atomic<int> bucket_index(0);
    char* buf_array[cpu_num];
    for (int i = 0; i < cpu_num; ++i) {
      buf_array[i] = (char *) memalign(4096, 512 * 1024);
      threads.emplace_back([this, &bucket_index, buf = buf_array[i]]() {
//      threads.emplace_back([this, &bucket_index]() {

//        char* buf = (char *) memalign(4096, 512 * 1024);
        while (true) {
          int bid = bucket_index++;
          if (bid >= BUCKET_NUM) {
            break;
          }

          buckets_[bid].LoadIndex(buf);

        }
//        free(buf);
      });
    }

    for (int i = 0; i < cpu_num; ++i) {
      threads[i].join();
      free(buf_array[i]);
    }
  }

  void EngineRace::OpenFiles() {
    assert(BUCKET_NUM%GROUP_NUM == 0);
    int imode = 0;
    if (is_new_) {
      imode |= USE_DIRECT|O_NOATIME | O_RDWR | O_CREAT;
    } else {
      imode |= USE_DIRECT|O_NOATIME | O_RDONLY | O_CREAT;
    }
    for (int i = 0; i < GROUP_NUM; ++i) {
      std::string keyDataFileName = dir_ + "/key" + std::to_string(i) + ".data";
      std::string valueDataFileName = dir_ + "/value" + std::to_string(i) + ".data";


      key_fd_arr_[i] = open(keyDataFileName.c_str(), imode, 0644);
      value_fd_arr_[i] = open(valueDataFileName.c_str(), imode, 0644);
      if (key_fd_arr_[i] < 0 || value_fd_arr_[i] < 0) {
        log_error("open file failed: %s", strerror(errno));
        exit(-1);
      }
      if (is_new_) {
        if(ftruncate64(value_fd_arr_[i], BUCKET_NUM_IN_GROUP * BUCKET_VALUE_MAX_SIZE) != 0) {
          log_error("file truncate failed: %s", strerror(errno));
          exit(-1);
        }
        if(ftruncate64(key_fd_arr_[i], BUCKET_NUM_IN_GROUP * BUCKET_KEY_MAX_SIZE) != 0) {
          log_error("file truncate failed: %s", strerror(errno));
          exit(-1);
        }
        int num = i+1;
//        pwrite64(value_fd_arr_[i], (char*)&num, 4096, 0);
//        pwrite64(key_fd_arr_[i], (char*)&num, 4096, 0);

      }
    }
  }


}  // namespace polar_race
