//
// Created by xuzhe on 11/4/18.
//

#ifndef DBDEMO_INDEX_H
#define DBDEMO_INDEX_H

#include <cstdint>
struct Index{
    uint64_t key;
    uint32_t block_index;
    Index(uint64_t key, uint32_t index) {
      this->key = key;
      this->block_index = index;
    }
    Index() {}
    bool operator<(const Index &other) const {
      if(key == other.key) return block_index > other.block_index;
      else return key < other.key;
    }
};


#endif //DBDEMO_INDEX_H
