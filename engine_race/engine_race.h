// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_


#include <unordered_map>
#include <map>
#include "include/engine.h"
#include <atomic>
#include <sys/time.h>
#include <thread>
#include <iostream>
#include <vector>
#include <condition_variable>
#include "util.h"
#include "Bucket.h"
#include "config.h"
#include "mapped_buffer_manager.h"
#include "range_loader.h"

namespace polar_race {


  class EngineRace : public Engine {
    public:
    friend class Bucket;
    friend class RangeLoader;
    static RetCode Open(const std::string &name, Engine **eptr);

    explicit EngineRace(const std::string &dir, bool is_new);

    ~EngineRace();

    RetCode Write(const PolarString &key,
                  const PolarString &value) override;

    RetCode Read(const PolarString &key,
                 std::string *value) override;

    /*
     * NOTICE: Implement 'Range' in quarter-final,
     *         you can skip it in preliminary.
     */
    RetCode Range(const PolarString &lower,
                  const PolarString &upper,
                  Visitor &visitor) override;

    private:
    std::string dir_;
    bool is_new_;
    bool first_;

    std::mutex mutex_;
    MappedBufferManager mappedBufferManager;
    RangeLoader rangeLoader_;
    Bucket buckets_[BUCKET_NUM];
    std::atomic<int> tid_num_;
    int value_fd_arr_[GROUP_NUM];
    int key_fd_arr_[GROUP_NUM];
    void Init();
    void OpenFiles();
    void PrintFirstOpTimestamp();
  };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
