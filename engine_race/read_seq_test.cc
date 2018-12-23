//
// Created by root on 12/4/18.
//

#include "read_seq_test.h"
#include "util.h"
#include "log.h"
#include <thread>
#include <vector>
#include <malloc.h>
#include <atomic>
namespace polar_race {
  void read_seq_test::start() {
    std::vector<std::thread> threads;
    std::atomic<uint64_t> total(0);
    double t1 = NowMicros();
    const int thread_num = 1;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([this, i, &total]() {
        uint64_t total_file_size = 0;
//        char *buf = (char *) memalign(512, 62 * 1024 * 1024);
        for (int j = i; j < BUCKET_NUM; j += thread_num) {
//          total_file_size += buckets_[j]->ReadWholeFile();
        }
        total += total_file_size;
      });
    }

    for (int i = 0; i < thread_num; ++i) {
      threads[i].join();
    }
    double t2 = NowMicros();
    log_debug("read: %s time: %.3lf speed: %s mem use: %s", ConvertFileSize(total).c_str(),
              (t2 - t1) / 1000000, ConvertFileSize(total / ((t2 - t1) / 1000000)).c_str(),
              ConvertFileSize(get_memory_usage() * 1024L).c_str());
    exit(-1);
  }

}