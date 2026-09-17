#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

#include "base/data_item.h"
#include "common.h"
#include "config.h"
#include "util/fast_random.h"
#include "util/json_config.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "storage/blink_tree/blink_tree.h"
#include "storage/fsm_tree/s_fsm_tree.h"
#include "dtx/dtx.h"
#include "util/zipfan.h"

// CRUD 四类事务类型（负载模式真实增删改查）
#define YCSB_TX_TYPES 4
const std::string YCSB_TX_NAME[YCSB_TX_TYPES] = {"Read", "Update", "Insert", "Delete"};

union user_table_key_t {
  uint64_t user_id;
  uint64_t item_key;

  user_table_key_t() {
    item_key = 0;
  }
};

// 编译阶段检查
static_assert(sizeof(user_table_key_t) == sizeof(uint64_t), "");

// magic ，可以简单验证下读取上来的是这个表的数据
#define YCSB_MAGIC 123
#define ycsb_user_table_magic (YCSB_MAGIC + 2)

// 表结构，magic 是用来检验的
struct ycsb_user_table_val {
    uint32_t magic;
    char file_0[100];
    char file_1[100];
    char file_2[100];
    char file_3[100];
    char file_4[100];
    char file_5[100];
    char file_6[100];
    char file_7[100];
    char file_8[100];
    char file_9[100];
};

class YCSB {
public:
    YCSB(RmManager* rm_manage_ , int record_cnt , int hot_record_cnt_ , int access_pattern_ 
        , std::vector<int> page_num_per_node , int read_cnt = 10 , int update_cnt = 90 , int filed_len_ = 100 , int TX_HOT_ = 60
        // CRUD 扩展参数：四类事务比例（和为100启用CRUD模式）+ 全局线程数（键分区）
        , int crud_read_p = 0 , int crud_update_p = 0 , int crud_insert_p = 0 , int crud_delete_p = 0
        , int total_threads = 0)
        :rm_manager(rm_manage_),
         record_count(record_cnt),
         access_pattern(access_pattern_),
         read_percent(read_cnt),
         update_percent(update_cnt),
         field_len(filed_len_),
         hot_record_cnt(hot_record_cnt_),
         tx_hot_rate(TX_HOT_){
        assert(read_cnt + update_cnt == 100);
        int total_keys = 10;
        now_account.store(record_cnt + 1);
        // 下面这个看着挺复杂的，其实就是 total_keys * (read_percent / 100.0)
        read_op_per_txn = std::max(0 , std::min(total_keys , (int)std::round(total_keys * (read_percent / 100.0))));
        write_op_per_txn = total_keys - read_op_per_txn;
        rw_flags = std::vector<bool>(total_keys , false);
        for (int i = read_op_per_txn ; i < total_keys ; i++){
            rw_flags[i] = true;
        }

        tuple_size = sizeof(DataItem) + sizeof(ycsb_user_table_val);

        num_records_per_page = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (tuple_size + sizeof(itemkey_t)) * BITMAP_WIDTH);
        num_pages = (record_count + num_records_per_page - 1) / num_records_per_page;

        // ---- CRUD 模式初始化：驱动端维护存活键视图（每全局线程一个独立分区） ----
        crud_percent[0] = crud_read_p;
        crud_percent[1] = crud_update_p;
        crud_percent[2] = crud_insert_p;
        crud_percent[3] = crud_delete_p;
        crud_enabled = (crud_read_p + crud_update_p + crud_insert_p + crud_delete_p) == 100
                       && total_threads > 0;
        if (crud_enabled){
            assert(record_count >= total_threads);
            per_thread_keys.resize(total_threads);
            per_thread_new_key_cnt.assign(total_threads , 0);
            // 初始键 [0, record_count) 按全局线程均分：线程 t 独占自己的区间，
            // 读/更/删互不冲突；跨节点/跨线程冲突只发生在页级锁，由锁协议处理
            uint64_t chunk = (uint64_t)record_count / total_threads;
            for (int t = 0 ; t < total_threads ; t++){
                uint64_t begin = (uint64_t)t * chunk;
                uint64_t end = (t == total_threads - 1) ? (uint64_t)record_count : (uint64_t)(t + 1) * chunk;
                per_thread_keys[t].reserve(end - begin + 16);
                for (uint64_t k = begin ; k < end ; k++){
                    per_thread_keys[t].push_back(k);
                }
            }
        }

        // 在整个项目会创建两个 YCSB 实例，一个是在存储层初始化，导入数据的时候，一个是在计算层，用来生成 YCSB 负载
        if (rm_manage_){
            // 存储层初始化 BLink
            bl_indexes.emplace_back(new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , 10000 , "ycsb"));

            // fsm
            fsm_trees.emplace_back(new S_SecFSM(rm_manager->get_diskmanager(),rm_manager->get_bufferPoolManager() , 20000 , "ycsb"));
            const uint32_t capacity = std::getenv("HCM_FSM_HEAP_PAGES")
                ? static_cast<uint32_t>(std::stoul(std::getenv("HCM_FSM_HEAP_PAGES")))
                : std::max(4096, num_pages * 3 + 1);
            if (!fsm_trees[0]->initialize(20000, capacity))
                throw std::runtime_error("YCSB FSM initialization failed");
        }else {
            // 计算层初始化 Zipfan
            zip_fans.reserve(ComputeNodeCount);
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                // 目前 YCSB 只有一个表，所以 zipfans 的结构是 ComputeNodeCount 行 + 1 列
                std::vector<ZipFanGen> zipfan_vec;
                uint64_t zipf_seed = 2 * GetCPUCycle() * (int)(ramdom_string(20)[0] % ComputeNodeCount);
                uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
                zipfan_vec.emplace_back(ZipFanGen(page_num_per_node[i] , 0.70 , zipf_seed & zipf_seed_mask));
                zip_fans.emplace_back(zipfan_vec);
            }
        }

        bench_name = "ycsb";
    }

    ~YCSB() = default; 

    // 给存储层用的，用来构建初始的表数据和 B+ 树
    void LoadTable(){
        PopulateUserTable();
    }
    void VerifyData();

    // 事务生成函数，生成多个读集和写集
    bool YCSB_Multi_RW(uint64_t *seed , tx_id_t tx_id , DTX *dtx , coro_yield_t& yield , bool is_partitioned = false){
        dtx->TxBegin(tx_id);

        // 1. 生成 10 个 key，放在 vec 里
        std::vector<itemkey_t> keys(10);
        generate_ten_keys(keys , seed , is_partitioned , dtx);

        for (int i = 0 ; i < 10 ; i++){
            if (rw_flags[i]){
                // 读事务
                auto ro_user_id = std::make_shared<DataItem>(0);
                dtx->AddToReadOnlySet(ro_user_id , keys[i]);
            }else {
                auto rw_user_id = std::make_shared<DataItem>(0);
                dtx->AddToReadWriteSet(rw_user_id , keys[i]);
            }
        }

        // 现在的 insert 和 delete 应该是不会回滚的
        if (!(dtx->TxExe(yield))){
            return false;
        }
        
        for (auto& item : dtx->read_only_set) {
            if (item.second.is_fetched) {
                ycsb_user_table_val* val = (ycsb_user_table_val*)item.second.item_ptr->value;
                assert(val);
                if (val->magic != ycsb_user_table_magic){
                    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
                    assert(false);
                }
            }
        }

        for (auto& item : dtx->read_write_set) {
            if (item.second.is_fetched) {
                ycsb_user_table_val* val = (ycsb_user_table_val*)item.second.item_ptr->value;
                if (val->magic != ycsb_user_table_magic){
                    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
                    assert(false);
                }
                // 写 item 的 file_0 为随机的字符串
                std::string rand_str = ramdom_string(field_len); 
                memcpy(val->file_0, rand_str.c_str(), field_len);
            }
        }

        bool commit_stat = dtx->TxCommit(yield);
        return commit_stat;
    }

    // ================= CRUD 扩展：真实四类事务负载 =================
    // 单键事务：READ/UPDATE/INSERT/DELETE，全部走 DTX→BLink/heap/FSM→WAL→提交链路。
    // 返回是否提交成功；tx_type 输出实际执行的事务类型（供统计）
    bool YCSB_CRUD_Tx(uint64_t *seed , tx_id_t tx_id , DTX *dtx , coro_yield_t& yield , int &tx_type){
        // 按比例选类型（40/20/20/20 或配置值）
        int r = FastRand(seed) % 100;
        int acc = 0;
        tx_type = 3;
        for (int t = 0 ; t < 4 ; t++){
            acc += crud_percent[t];
            if (r < acc){ tx_type = t; break; }
        }

        dtx->TxBegin(tx_id);

        // 本线程的存活键视图（每线程独立，无需加锁）
        int tid = dtx->t_id;
        assert(tid >= 0 && tid < (int)per_thread_keys.size());
        std::vector<itemkey_t> &live = per_thread_keys[tid];

        // 键耗尽保护：无键可读/更/删时降级为 insert（保证四类均非零且不空转）
        if (tx_type != 2 && live.empty()){
            tx_type = 2;
        }

        switch (tx_type){
            case 0: {  // READ：点查，经 BLink → RID → heap
                itemkey_t key = live[FastRand(seed) % live.size()];
                auto ro_item = std::make_shared<DataItem>(0);
                dtx->AddToReadOnlySet(ro_item , key);

                if (!(dtx->TxExe(yield))) return false;

                for (auto& item : dtx->read_only_set) {
                    if (item.second.is_fetched) {
                        ycsb_user_table_val* val = (ycsb_user_table_val*)item.second.item_ptr->value;
                        assert(val);
                        if (val->magic != ycsb_user_table_magic){
                            LOG(FATAL) << "[FATAL] CRUD read unmatch, tid-txid: " << dtx->t_id << "-" << tx_id;
                            assert(false);
                        }
                    }
                }
                return dtx->TxCommit(yield);
            }
            case 1: {  // UPDATE：修改非键字段 file_0
                itemkey_t key = live[FastRand(seed) % live.size()];
                auto rw_item = std::make_shared<DataItem>(0);
                dtx->AddToReadWriteSet(rw_item , key);

                if (!(dtx->TxExe(yield))) return false;

                for (auto& item : dtx->read_write_set) {
                    if (item.second.is_fetched) {
                        ycsb_user_table_val* val = (ycsb_user_table_val*)item.second.item_ptr->value;
                        if (val->magic != ycsb_user_table_magic){
                            LOG(FATAL) << "[FATAL] CRUD update read-unmatch, tid-txid: " << dtx->t_id << "-" << tx_id;
                            assert(false);
                        }
                        std::string rand_str = ramdom_string(field_len);
                        memcpy(val->file_0, rand_str.c_str(), field_len);
                    }
                }
                return dtx->TxCommit(yield);
            }
            case 2: {  // INSERT：新键（本线程独立键区间，永不冲突）
                itemkey_t key = crud_new_key_base + (uint64_t)tid * crud_new_key_stride
                                + (per_thread_new_key_cnt[tid])++;
                auto ins_item = std::make_shared<DataItem>(0 , (int)sizeof(ycsb_user_table_val));
                ycsb_user_table_val* val = (ycsb_user_table_val*)ins_item->value;
                val->magic = ycsb_user_table_magic;
                std::string f = ramdom_string(field_len);
                strncpy(val->file_0 , f.c_str() , sizeof(val->file_0));
                for (int i = 1 ; i < 10 ; i++){
                    strncpy(val->file_0 + i * sizeof(val->file_0) , f.c_str() , sizeof(val->file_0));
                }

                dtx->AddToInsertSet(ins_item , key);

                if (!(dtx->TxExe(yield))) return false;

                bool ok = dtx->TxCommit(yield);
                if (ok){
                    // 提交成功才纳入存活键集合（结果未知/失败不算存活）
                    live.push_back(key);
                }
                return ok;
            }
            default: {  // DELETE：随机存活键；不存在键（被并发删除）→ 确定性 abort
                size_t idx = FastRand(seed) % live.size();
                itemkey_t key = live[idx];
                auto del_item = std::make_shared<DataItem>(0);
                dtx->AddToDeleteSet(del_item , key);

                if (!(dtx->TxExe(yield))) return false;

                bool ok = dtx->TxCommit(yield);
                if (ok){
                    // 提交成功才从存活键集合移除（swap 删除 O(1)，顺序无关）
                    live[idx] = live.back();
                    live.pop_back();
                }
                return ok;
            }
        }
    }

    bool is_crud_enabled() const { return crud_enabled; }
    
    // 生成 10 个 key，生成时需要注意两个规则
    // 1. 是否是热点数据(ZipFian 不需要这个规则)
    // 2. 是否是跨分区访问数据
    void generate_ten_keys(std::vector<itemkey_t> &keys , uint64_t *seed , bool is_partitioned , const DTX *dtx){
        int belonged_node_id;
        int target_node_id;
        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            belonged_node_id = dtx->compute_server->get_node()->ts_cnt;
        }else {
            belonged_node_id = dtx->compute_server->getNodeID();
        }
       
        page_id_t page_id;
        
        if (ComputeNodeCount == 1){
            // 如果只有一个节点，那就无所谓是否分区了
            target_node_id = dtx->compute_server->getNodeID();
        }else if (is_partitioned){
            do {
                target_node_id = FastRand(seed) % ComputeNodeCount;
            }while(target_node_id == belonged_node_id);
        }else {
            target_node_id = belonged_node_id;
        }

        int partition_size = dtx->compute_server->get_node()->getMetaManager()->GetPartitionSizePerTable(0);
        int now_page_num = dtx->compute_server->get_node()->getMetaManager()->GetTablePageNum(0);
        int par_cnt = now_page_num / partition_size + 1;
        int node_page_num;

        for (int i = 0 ; i < 10 ; i++){
            if (access_pattern == 0){
                // 根据 is_partition 和 TX_HOT 以及热点事务的比例来生成一个 page_id
                node_page_num = dtx->compute_server->get_node()->getMetaManager()->GetPageNumPerNode(target_node_id , 0 , ComputeNodeCount);
                int num_hot_this_node = (int)((double)node_page_num * ((double)hot_record_cnt / (double)record_count));
                if (FastRand(seed) % 100 < tx_hot_rate){
                    // 热点事务，需要访问热点页面
                    page_id = FastRand(seed) % num_hot_this_node;
                }else {
                    // 访问冷页面
                    page_id = (FastRand(seed) % (node_page_num - num_hot_this_node)) + num_hot_this_node;
                }
                // LOG(INFO) << "Hot Cnt = " << num_hot_this_node << " total page num = " << node_page_num;
            } else if (access_pattern == 1){
                // zipfan 本身就带了热点属性，所以只需要考虑分区即可    
                page_id = zip_fans[target_node_id][0].next() + 1;
            } else {
                assert(false);
            }

            int debug_page_id = page_id;
            
            // 前面得到的 page_id 是逻辑上的 page_id，表示的是页面在本节点管理分区内的偏移量，需要再映射到具体的页面上
            // 举个例子，分区大小 1000，三个节点，然后 page_id = 1020，那映射到之后的 page_id 就是 1000 + 1000 + 1000 + 20 = 3020
            // 在比如 page_id = 3020，那映射之后就是 9020
            page_id = (page_id / partition_size) * (ComputeNodeCount * partition_size)
                    + (target_node_id * partition_size)
                    + page_id % partition_size
                    + 1;
            
            assert(page_id > 0);
            assert(page_id <= now_page_num);

            // int tuple_size = sizeof(DataItem) + sizeof(ycsb_user_table_val);
            int account_cnt_per_page = PAGE_SIZE / tuple_size;
            keys[i] = (page_id - 1) * account_cnt_per_page + (FastRand(seed) % account_cnt_per_page);
        }
    }

public:
    int getRecordCount() const {
        return record_count;
    }
    int getAccessPattern() const {
        return access_pattern;
    }
    int getReadPercent() const {
        return read_percent;
    }
    int getUpdatePercent() const {
        return update_percent;
    }
    int getFiledLen() const {
        return field_len;
    }

private:
    void PopulateUserTable();
    void LoadRecord(RmFileHandle *file_handle ,
        itemkey_t item_key , void *val_ptr , 
        size_t val_size , table_id_t table_id ,
        std::ostream &index_file);

private:
    RmManager* rm_manager;
    std::string bench_name;
    std::vector<S_BLinkIndexHandle*> bl_indexes;
    std::vector<S_SecFSM*> fsm_trees;

private:
    int record_count;           // 总记录数量
    int access_pattern;         // 0：每个页面被访问的概率一样，1：zipfian
    int read_percent;           // 读比例
    int update_percent;         // 写比例
    int field_len;              // 每个字段的长度，默认 100
    int hot_record_cnt;         // 热点账户数量
    int tx_hot_rate;                 // 访问热点账户的事务占比

    int read_op_per_txn;        // 单个事务要做几次读操作，这个值是根据 read_percent 计算的
    int write_op_per_txn;       // 同上
    std::vector<bool> rw_flags; // 假如 read_op_per_txn = 9 , write_op_per_txn = 1，那这个数组的值就是 0000000001

    int tuple_size;

    // zip_fans[i][j]：第 i 个节点的第 j 个表的 zipfans 账户生成
    std::vector<std::vector<ZipFanGen>> zip_fans;

    std::atomic<int> now_account{0};

    // ---- CRUD 扩展状态 ----
    bool crud_enabled = false;
    int crud_percent[4] = {0 , 0 , 0 , 0};   // read/update/insert/delete 比例
    // 每全局线程的存活键视图（初始分区键 + 本线程已提交插入的新键 - 已提交删除的键）
    std::vector<std::vector<itemkey_t>> per_thread_keys;
    // 每全局线程的新键计数器（独立区间分配，永不冲突）
    std::vector<uint64_t> per_thread_new_key_cnt;
    static const uint64_t crud_new_key_base = 1000000000ULL;      // 新键基址，避开初始键区间
    static const uint64_t crud_new_key_stride = 1000000000ULL;    // 每线程键区间跨度

    //fsm 使用
    int num_records_per_page;
    int num_pages;
    
    const static std::string ramdom_string(int len){
        static thread_local std::mt19937 rng{std::random_device{}()};
        static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphanum) - 2);
        std::string s;

        s.reserve(len);
        for (int i = 0; i < len; ++i) {
            s.push_back(alphanum[dist(rng)]);
        }
        return s;
    }
    
};
