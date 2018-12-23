//
// Created by root on 12/4/18.
//

#ifndef DBDEMO_MAPPED_BUFFER_MANAGER_H
#define DBDEMO_MAPPED_BUFFER_MANAGER_H

#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include "util.h"
#include "log.h"
#include "config.h"
namespace polar_race {
  class MappedBufferManager {
    public:
    MappedBufferManager(const std::string &filename, bool is_new);
    void Init();

    char* GetKeyBuffer(int id);

    char* GetValueBuffer(int id);

    uint32_t* GetSizePtr(int id);

    private:
    const std::string filename_;
    int fd_;
    bool is_new_;
    char* map_base_;
    char* key_buffer_base_;
    char* value_buffer_base_;
    uint32_t* size_base_;
  };
}

#endif //DBDEMO_MAPPER_BUFFER_MANAGER_H
