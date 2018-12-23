//
// Created by root on 12/5/18.
//

#ifndef ENGINE_XUZHE_FIRST_RANGE_LOADER_H
#define ENGINE_XUZHE_FIRST_RANGE_LOADER_H

//#include "engine_race.h"
#include <deque>
#include <vector>
#include <malloc.h>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "config.h"
namespace polar_race {

  //
  struct ReadTask {
    int bid;
    uint32_t offset;
    uint32_t len;

    ReadTask(int bid, uint32_t offset, uint32_t len)
    : bid(bid), offset(offset), len(len) {}
  };
  class EngineRace;
  class RangeLoader {
    private:
    EngineRace* db_;
    std::deque<char*> buffer_pool_;
    std::deque<ReadTask> task_queue_;
    std::mutex mutex_;
    std::mutex buffer_pool_mutex_;
    std::condition_variable pool_empty_cv_;
    std::condition_variable start_cv_;
    std::condition_variable all_done_cv_;
    std::vector<int> read_index_;
    std::atomic<int> range_count_;
    std::atomic<bool> range_flag_;

    int last_not_empty_id_;
    void Run(int tid);
    void PushReadTaskToQueue();
    public:

    RangeLoader(EngineRace* db);
    void AddWatcher();
    void ReleaseBucket(int id);
    void WaitAllDone();

  };

}
#endif //ENGINE_XUZHE_FIRST_RANGE_LOADER_H
