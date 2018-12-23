//
// Created by root on 11/11/18.
//

#include "util.h"
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <cassert>

std::string ConvertFileSize(long size) {
  long kb = 1024;
  long mb = kb * 1024;
  long gb = mb * 1024;

  char buf[128];
  if (size >= gb) {
    sprintf(buf, "%9.3f GB", (float) size / gb);
  } else if (size >= mb) {
    float f = (float) size / mb;
    if(f > 100) {
      sprintf(buf, "%9.3f MB", f);
    } else {
      sprintf(buf, "%9.3f MB", f);
    }
  } else if (size >= kb) {
    float f = (float) size / kb;
    if(f > 100) {
      sprintf(buf, "%9.3f KB", f);
    } else {
      sprintf(buf, "%9.3f KB", f);
    }
  } else
    sprintf(buf, "%ld B", size);
  return std::string(buf);
}

std::string DatetimeToString(time_t time) {
  tm *tm_ = localtime(&time);                // 将time_t格式转换为tm结构体
  int year, month, day, hour, minute, second;// 定义时间的各个int临时变量。
  year = tm_->tm_year + 1900;                // 临时变量，年，由于tm结构体存储的是从1900年开始的时间，所以临时变量int为tm_year加上1900。
  month = tm_->tm_mon + 1;                   // 临时变量，月，由于tm结构体的月份存储范围为0-11，所以临时变量int为tm_mon加上1。
  day = tm_->tm_mday;                        // 临时变量，日。
  hour = tm_->tm_hour;                       // 临时变量，时。
  minute = tm_->tm_min;                      // 临时变量，分。
  second = tm_->tm_sec;                      // 临时变量，秒。
  char yearStr[5], monthStr[3], dayStr[3], hourStr[3], minuteStr[3], secondStr[3];// 定义时间的各个char*变量。
  sprintf(yearStr, "%d", year);              // 年。
  sprintf(monthStr, "%d", month);            // 月。
  sprintf(dayStr, "%d", day);                // 日。
  sprintf(hourStr, "%d", hour);              // 时。
  sprintf(minuteStr, "%d", minute);          // 分。
  if (minuteStr[1] == '\0')                  // 如果分为一位，如5，则需要转换字符串为两位，如05。
  {
    minuteStr[2] = '\0';
    minuteStr[1] = minuteStr[0];
    minuteStr[0] = '0';
  }
  sprintf(secondStr, "%d", second);          // 秒。
  if (secondStr[1] == '\0')                  // 如果秒为一位，如5，则需要转换字符串为两位，如05。
  {
    secondStr[2] = '\0';
    secondStr[1] = secondStr[0];
    secondStr[0] = '0';
  }
  char s[20];                                // 定义总日期时间char*变量。
  sprintf(s, "%s-%s-%s %s:%s:%s", yearStr, monthStr, dayStr, hourStr, minuteStr, secondStr);// 将年月日时分秒合并。
  std::string str(s);                             // 定义string变量，并将总日期时间char*变量作为构造函数的参数传入。
  return str;                                // 返回转换日期时间后的string变量。
}

void GetStdoutFromCommand(std::string taskName, std::string cmd, bool printTime) {
  FILE *stream;
  const int max_buffer = 256;
  char buffer[max_buffer];
  cmd.append(" 2>&1");

  stream = popen(cmd.c_str(), "r");
  if (stream) {
    int cnt = 0;
    while (!feof(stream)) {
      memset(buffer, 0, sizeof(buffer));
      if (fgets(buffer, max_buffer, stream) != NULL) {
        if (printTime) {
          std::string timeStr = DatetimeToString(time(0));
          fprintf(stderr, "%s %4d  %s  %s", taskName.c_str(), cnt, timeStr.c_str(), buffer);
        } else {
          fprintf(stderr, "%s %4d  %s", taskName.c_str(), cnt, buffer);
        }
      }

      cnt++;
    }
    pclose(stream);
  }
}


uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

std::string hexdump(const char* data, size_t size) {
  static char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
  std::string str;

  for (size_t i = 0; i < size; ++i) {
    char ch = data[i];
    str += hex_chars[ ( ch & 0xF0 ) >> 4 ];
    str += hex_chars[ ( ch & 0x0F ) >> 0 ];
  }
  return str;
}

void read_file(const char* name) {
  int f = open(name, O_RDONLY);
  if(f < 0) {
    fprintf(stderr, "open file error(%d) : %s\n", errno, name);
    return;
  }
  char buf[4096*100];
  int count = 0;
  while(true) {
    bzero(buf, sizeof(buf));
    ssize_t r = read(f, buf, sizeof(buf));
    count++;
    if(r <= 0) {
      break;
    }
    if(count > 100000) break;
    fprintf(stderr, "%s %4d  %s", name, count, buf);

  }
  fflush(stderr);
  close(f);
}


long to_long(const char* data) {
//  long l = *(data+7) & 0xff;
//  l = (l<<8) | (*(data+6) & 0xff);
//  l = (l<<8) | (*(data+5) & 0xff);
//  l = (l<<8) | (*(data+4) & 0xff);
//  l = (l<<8) | (*(data+3) & 0xff);
//  l = (l<<8) | (*(data+2) & 0xff);
//  l = (l<<8) | (*(data+1) & 0xff);
//  l = (l<<8) | (*(data) & 0xff);
  long l = *(long*)data;
  return l;
}

uint64_t to_uint64(const char* data) {
  uint64_t u = *(uint64_t*)data;
  u = __bswap_64(u);
  return u;
}

bool file_exist(const char* file_path)
{
  if((access(file_path, F_OK))!=-1)
    return true;
  else
    return false;
}

int get_memory_usage() {
  int fd, data;
  char buf[4096];
  char *vm;

  if ((fd = open("/proc/self/status", O_RDONLY)) < 0)
    return -1;

  read(fd, buf, 4095);
  buf[4095] = '\0';
  close(fd);

  vm = strstr(buf, "VmRSS:");
  if (vm) {
    sscanf(vm, "%*s %d", &data);
  }
  return data;
}

int GetFileLength(const std::string& file) {
  struct stat stat_buf;
  int rc = stat(file.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}




