//
// Created by root on 12/4/18.
//

#ifndef DBDEMO_READSEQTEST_H
#define DBDEMO_READSEQTEST_H


#include "Bucket.h"
namespace polar_race {
  class read_seq_test {
    public:
    read_seq_test(std::vector<Bucket*>& buckets) : buckets_(buckets) {

    }

    void start();


    private:
    std::vector<Bucket*>& buckets_;
  };
}

#endif //DBDEMO_READSEQTEST_H
