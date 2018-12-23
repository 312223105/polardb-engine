//
// Created by root on 11/11/18.
//

#ifndef DBDEMO_UTIL_H
#define DBDEMO_UTIL_H

#include <sys/time.h>
#include <string>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory.h>
#include "config.h"

std::string ConvertFileSize(long size);
uint64_t NowMicros();
std::string DatetimeToString(time_t time);
void GetStdoutFromCommand(std::string taskName, std::string cmd, bool printTime=false);
std::string hexdump(const char* data, size_t size);
void read_file(const char* name);
long to_long(const char* data);
uint64_t to_uint64(const char* data);
bool file_exist(const char* file_path);
int get_memory_usage();
int GetFileLength(const std::string& file);
//unsigned long get_file_size(const char *filename);
inline uint32_t GetGroupId(uint32_t bid) {
  return bid / BUCKET_NUM_IN_GROUP;
}

inline uint64_t GetGroupValueFileOffset(uint32_t bid, uint64_t offset) {
  return (bid % BUCKET_NUM_IN_GROUP) * BUCKET_VALUE_MAX_SIZE + offset;
}

inline uint64_t GetGroupKeyFileOffset(uint32_t bid, uint64_t offset) {
  return (bid % BUCKET_NUM_IN_GROUP) * BUCKET_KEY_MAX_SIZE + offset;
}
#endif //DBDEMO_UTIL_H
