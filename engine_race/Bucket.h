//
// Created by root on 11/13/18.
//

#ifndef DBDEMO_BUCKET_H
#define DBDEMO_BUCKET_H

#include <sys/stat.h>
#include <sys/fcntl.h>
#include <mutex>
#include <condition_variable>
#include "config.h"
#include "index.h"
#include "include/engine.h"
#include <vector>
#include <atomic>
#include "mapped_buffer_manager.h"

namespace polar_race {

  class EngineRace;

  class RangeLoader;

  class Bucket {
    friend class EngineRace;

    friend class RangeLoader;

    public:
    Bucket() : data_loaded_(false), ref_count_(0), read_part_remain_(RANGE_READ_PART),
               read_flag_(0) {}

    Bucket(const Bucket &) = delete;

    Bucket &operator=(const Bucket &) = delete;

    ~Bucket() = default;

    void Write(const char *key, const char *value);

    RetCode Read(const char *key, std::string *value);

    uint32_t GetEntryCount();

    char* GetData();

    void LoadFile(char *buf, uint64_t offset, uint64_t len);

    private:
    std::vector<Index> index_;
    std::mutex mutex_;
    std::condition_variable data_ready_cv_;
    uint32_t id_;
    std::string dir_;
    int key_data_fd_;
    int value_data_fd_;

    char *key_buffer_;
    char *value_buffer_;
    uint32_t *entry_count_;

    bool index_loaded_;
    bool data_loaded_;
    char *data_;
    EngineRace *db_;

    std::atomic<int> ref_count_;
    std::atomic<int> read_part_remain_;

    std::atomic<int> read_flag_;
    bool is_new_;

    void Init(EngineRace *db, uint32_t id, bool is_new, int key_fd, int value_fd);

    void LoadIndex(char *buf);

  };
}
#endif //DBDEMO_BUCKET_H
