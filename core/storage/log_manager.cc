#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "log_manager.h"

LogManager::LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name)
        :disk_manager_(disk_manager), log_replay_(log_replay) {
    if(log_file_name != LOG_FILE_NAME){
        if(disk_manager_->is_file(log_file_name)) {
            disk_manager_->destroy_file(log_file_name);
        }
        disk_manager_->create_file(log_file_name);
    }
    log_file_fd_ = disk_manager_->open_file(log_file_name);
}

void LogManager::write_batch_log_to_disk(std::string batch_log) {
    log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    assert(bytes_write == (ssize_t)(batch_log.length() * sizeof(char)));
    
    // 强制刷盘，确保数据落到物理磁盘
    fdatasync(log_file_fd_);

    log_replay_->add_max_replay_off_(bytes_write);
}

void LogManager::write_raft_log_to_disk(std::string batch_log){
    assert(log_file_fd_ > 0);
    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    // std::this_thread::sleep_for(std::chrono::milliseconds(2));
    assert(bytes_write == (ssize_t)(batch_log.length() * sizeof(char)));
    
    // 强制刷盘，确保数据落到物理磁盘
    fdatasync(log_file_fd_);
    // fsync(log_file_fd_);
}

void LogManager::write_batch_log_to_disk(char* batch_log, size_t size) {
    if (log_file_fd_ == -1) {
        log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    }

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log, size);
    assert(bytes_write == (ssize_t)size);
    
    // 强制刷盘，确保数据落到物理磁盘
    fdatasync(log_file_fd_);
    // fsync(log_file_fd_);

    // RDMA_// LOG(INFO) << "Write batch log's size is " << bytes_write;

    log_replay_->add_max_replay_off_(bytes_write);
}