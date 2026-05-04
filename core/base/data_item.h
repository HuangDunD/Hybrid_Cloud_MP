#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <stddef.h>
#include <butil/logging.h>

#include "common.h"
#include "config.h"

struct DataItem {
  table_id_t table_id;

  lock_t lock;
  uint8_t *value;
  int value_size;

  uint64_t version;     // Version number
  lsn_t prev_lsn;       // previous lsn, for undo to find the previous version
  uint8_t valid;        // 1: Not deleted, 0: Deleted
  uint8_t user_insert;  // 1：本元组在事务中被删除了
  tx_id_t timeStamp;    // 最后一个对本元组加写锁 / 更新本元组的事务 ID，用于识别“本事务自持写锁”
  tx_id_t commitTimeStamp; // 最后一次成功提交对本元组写入的事务的提交时间戳

  DataItem() : table_id(0), lock(0), value(nullptr), value_size(0), version(0), prev_lsn(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {}

  // For server load data
  DataItem(table_id_t t , uint8_t* d , int val_size) : table_id(t), lock(0), version(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {
      value_size = val_size;
      value = new uint8_t[value_size];
      memcpy(value , d , value_size);
  }

  // Build an empty item for fetching data from remote
  DataItem(table_id_t t)
      : table_id(t), value_size(0),  value(nullptr) , lock(0), version(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {}
  DataItem(table_id_t t , int val_size)
      : table_id(t), lock(0), value(nullptr), value_size(val_size), version(0), prev_lsn(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {
    value = new uint8_t[value_size];
  }
  // Accept size_t to avoid overload ambiguity when callers pass sizeof(...)
  DataItem(table_id_t t , size_t val_size)
      : table_id(t), lock(0), value(nullptr), value_size(static_cast<int>(val_size)), version(0), prev_lsn(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {
    value = new uint8_t[value_size];
  }
  // For user insert item
  DataItem(table_id_t t, size_t s , uint8_t ins)
      : table_id(t), value_size(s),  lock(0), version(0), valid(1), user_insert(ins), timeStamp(0), commitTimeStamp(0) {
    value = new uint8_t[value_size];
  }
  // For server load data
  DataItem(table_id_t t, size_t s , uint8_t* d) : table_id(t), value_size(s), lock(0), version(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0) {
    value = new uint8_t[value_size];
    memcpy(value, d, s);
  }

  // 给 SQL 解析用的，只需要填入个值即可
  DataItem(int len , bool is_val) : table_id(0), lock(0), value_size(len), version(0), prev_lsn(0), valid(1), user_insert(0), timeStamp(0), commitTimeStamp(0){
    value = new uint8_t[value_size];
  }

  ~DataItem() {
    if (value != nullptr) {
        delete[] value;
        value = nullptr;
    }
  }

  ALWAYS_INLINE
  size_t GetSerializeSize() const {
    return sizeof(DataItem) + value_size;
  }

  // 把这个结构体的数据序列华为一个字符串
  // 序列化的结构是：各个参数 + 最后的元组
  ALWAYS_INLINE
  void Serialize(char* undo_buffer) {
    memcpy(undo_buffer, (char*)this, sizeof(DataItem));
    memcpy(undo_buffer + sizeof(DataItem), value, value_size);
  }

  DataItem(const DataItem& other)
      : table_id(other.table_id),
        lock(other.lock),
        value_size(other.value_size),
        version(other.version),
        prev_lsn(other.prev_lsn),
        valid(other.valid),
        user_insert(other.user_insert),
        timeStamp(other.timeStamp),
        commitTimeStamp(other.commitTimeStamp) {
    if (other.value != nullptr) {
      value = new uint8_t[value_size];
      memcpy(value, other.value, value_size);
    } else {
      value = nullptr;
    }
  }

  DataItem& operator=(const DataItem& other) {
    if (this == &other) {
      return *this; // 防止自赋值 (a = a)
    }

    // 先释放自己原有的内存，防止内存泄漏
    if (value != nullptr) {
      delete[] value;
    }

    // 拷贝所有普通成员变量
    table_id = other.table_id;
    lock = other.lock;
    value_size = other.value_size;
    version = other.version;
    prev_lsn = other.prev_lsn;
    valid = other.valid;
    user_insert = other.user_insert;
    timeStamp = other.timeStamp;
    commitTimeStamp = other.commitTimeStamp;

    // 深拷贝 value 指向的内存
    if (other.value != nullptr) {
      value = new uint8_t[value_size];
      memcpy(value, other.value, value_size);
    } else {
      value = nullptr;
    }

    return *this;
  }
};

// const size_t DataItemSize = sizeof(DataItem); // 32 bytes

using DataItemPtr = std::shared_ptr<DataItem>;