#pragma once

#include <fcntl.h>     
#include <sys/stat.h>  
#include <unistd.h>    
#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>

#include "common.h"
#include "core/fiber/thread.h"

class DiskManager {
public:
    typedef RWMutex RWMutexType;

public:
    explicit DiskManager(bool direct_page_reads = false);

    ~DiskManager();

    void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);

    void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

    // RPC full-page reads only. The destination must be PAGE_SIZE-aligned.
    // Wait for replay when target_lsn is newer than the file's page version.
    void read_page_for_fetch(int fd, page_id_t page_no, char *data, LLSN target_lsn = 0);
    bool direct_page_reads() const { return direct_page_reads_; }
    uint64_t page_read_count() const { return page_read_count_.load(); }
    uint64_t page_read_time_ns() const { return page_read_time_ns_.load(); }
    uint64_t page_read_lsn_retries() const { return page_read_lsn_retries_.load(); }

    // update part of page data, value_size: num_bytes
    void update_value(int fd, page_id_t page_no, int slot_offset, char* value, int value_size);

    // void set_bitmap(int fd, page_id_t page_no, int pos);

    // void reset_bitmap(int fd, page_id_t page_no, int pos);

    page_id_t allocate_page(int fd);

    void deallocate_page(page_id_t page_id);

    /*目录操作*/
    bool is_dir(const std::string &path);

    void create_dir(const std::string &path);

    void destroy_dir(const std::string &path);

    /*文件操作*/
    bool is_file(const std::string &path);

    void create_file(const std::string &path);

    void destroy_file(const std::string &path);

    int open_file(const std::string &path);

    void close_file(int fd);

    uint64_t get_file_size(const std::string &file_name);

    std::string get_file_name(int fd);

    int get_file_fd(const std::string &file_name);

    void SetLogFd(int log_fd) { log_fd_ = log_fd; }

    int GetLogFd() { return log_fd_; }

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) { fd2pageno_[fd] = start_page_no; }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数 
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) { return fd2pageno_[fd]; }

    static constexpr int MAX_FD = 30000;

private:
    int page_read_fd(int fd);
    const bool direct_page_reads_;
    std::unordered_map<int, int> direct_read_fds_;
    std::atomic<uint64_t> page_read_count_{0};
    std::atomic<uint64_t> page_read_time_ns_{0};
    std::atomic<uint64_t> page_read_lsn_retries_{0};
    // 文件打开列表，用于记录文件是否被打开
    std::unordered_map<std::string, int> path2fd_;  //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_;  //<Page fd,Page文件磁盘路径>哈希表
    
    int log_fd_ = -1;                             // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{};  // 文件中已经分配的页面个数，初始值为0

    RWMutexType mutex;  // 并发锁
};
