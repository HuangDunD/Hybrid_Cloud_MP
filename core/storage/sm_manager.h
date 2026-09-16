#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>
#include <vector>

#include "disk_manager.h"
#include "buffer/storage_bufferpool.h"
#include "sm_meta.h"
#include "record/rm_file_handle.h"
#include "context.h"
#include "core/fiber/thread.h"
#include "storage/blink_tree/blink_tree.h"

struct ColDef {
    std::string name;
    ColType type;
    int len;

    ColDef() {}
    ColDef(std::string name_ , ColType type_ , int len_){
        name = name_;
        type = type_;
        len = len_;
    }

    void serialize(char* dest, int& offset) {
        int name_size = name.length();
        memcpy(dest + offset, &name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, name.c_str(), name_size);
        offset += name_size;
        memcpy(dest + offset, &type, sizeof(ColType));
        offset += sizeof(ColType);
        memcpy(dest + offset, &len, sizeof(int));
        offset += sizeof(int);
    }

    void deserialize(char* src, int& offset) {
        int name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        name = std::string(src + offset, name_size);
        offset += name_size;

        type = *reinterpret_cast<const ColType*>(src + offset);
        offset += sizeof(ColType);

        len = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
    }
};


class SmManager{
public:
    typedef RWMutex RWMutexType;

public: 
    // 单个 SmManager 管理单个数据库
    DBMeta db;
    std::unordered_map<std::string , RmFileHandle*> m_fhs;

public:
    SmManager(RmManager *_rm_manager, StorageBufferPoolManager *buffer_)
        :rm_manager(_rm_manager) , buffer_pool_mgr(buffer_){
        db.m_name = "";
    }

    ~SmManager() = default;

    StorageBufferPoolManager *getBufferPoolMgr(){
        return buffer_pool_mgr;
    }

public:
    int create_db(const std::string &db_name);
    int create_primary(const std::string &table_name);
    int create_fsm(const std::string &tab_name , int tuple_size , table_id_t table_id);
    bool is_dir(const std::string &db_name);
    int drop_db(const std::string &db_name);
    int open_db(const std::string &db_name);
    int close_db();
    int flush_meta();

    // ==================== BLink 日志重放支持 ====================
    // 获取（或懒创建）某个 blink 索引文件的存储侧句柄，供 LogReplay
    // 重放 BLINKINSERT/BLINKDELETE 与 UndoForFailedNode 撤销使用。
    // 返回 nullptr 表示索引文件不存在（可能未建表/日志属于已删表）。
    // 线程安全：内部互斥；句柄自带操作锁（get_op_mutex）串行化 Redo/Undo。
    S_BLinkIndexHandle* GetOrCreateBLinkHandle(const std::string &blink_table_name);

    // open_db / drop_table 重建或删除 blink 文件后必须调用：
    // 旧句柄对应的文件已被销毁，全部失效
    void InvalidateAllBLinkHandles();

    std::string show_tables(Context *context);
    void desc_table(const std::string &table_name , Context *context);
    int create_table(const std::string &table_name , const std::vector<ColDef> &col_defs , const std::string &pri_key);
    int drop_table(const std::string &table_namet);
    int drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names);
public:
    std::string getIndexName(const std::string &tab_name , std::vector<int> cols , IndexType type){
        if (type == IndexType::BTREE_INDEX){
            std::stringstream primary_name_ss;
            primary_name_ss << tab_name;
            for (int i = 0 ; i < cols.size() ; i++){
                primary_name_ss << "_" << cols[i];
            }
            primary_name_ss << "_bl";

            return primary_name_ss.str();

        }else {
            assert(false);
        }
    }

    std::string getIndexName(const std::string &tab_name , std::vector<ColMeta> cols , IndexType type){
        if (type == IndexType::BTREE_INDEX){
            std::stringstream primary_name_ss;
            primary_name_ss << tab_name;
            for (int i = 0 ; i < cols.size() ; i++){
                primary_name_ss << "_" << cols[i].name;
            }
            primary_name_ss << "_bl";

            return primary_name_ss.str();
            
        }else {
            assert(false);
        }
    }
private:
    StorageBufferPoolManager *buffer_pool_mgr;
    RmManager* rm_manager;
    RWMutexType rw_mutex;

    // 存储侧 blink 索引句柄注册表（日志重放用）
    std::unordered_map<std::string, std::shared_ptr<S_BLinkIndexHandle>> s_blink_handles_;
    std::mutex s_blink_handles_mtx_;
};