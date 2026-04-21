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

#define YCSB_TX_TYPES 1     //只有一个事务类型

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
        , std::vector<int> page_num_per_node , std::vector<int> node_key_counts , int read_cnt = 10 , int update_cnt = 90 , int filed_len_ = 100 , int TX_HOT_ = 60 , double zipf_theta_ = 0.70 , bool random_generate_ = false)
        :rm_manager(rm_manage_),
         record_count(record_cnt),
         access_pattern(access_pattern_),
         read_percent(read_cnt),
         update_percent(update_cnt),
         field_len(filed_len_),
         hot_record_cnt(hot_record_cnt_),
         tx_hot_rate(TX_HOT_),
         zipf_theta(zipf_theta_),
         random_generate(random_generate_){
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

        // 在整个项目会创建两个 YCSB 实例，一个是在存储层初始化，导入数据的时候，一个是在计算层，用来生成 YCSB 负载
        if (rm_manage_){
            // 存储层初始化 BLink
            bl_indexes.emplace_back(new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , 10000 , "ycsb"));

            // fsm
            fsm_trees.emplace_back(new S_SecFSM(rm_manager->get_diskmanager(),rm_manager->get_bufferPoolManager() , 20000 , "ycsb"));
            fsm_trees[0]->initialize(20000 , num_pages * 3);
        }else {
            // 计算层初始化 Zipfan
            // 注意：Zipfian 的语义是「热点 key」。
            // n 为该节点持有的「真实 key 数量」(node_key_counts[i])，由 MetaManager 在 PrefetchIndex
            // 之后构建，无论 random_generate 为 true 还是 false 都准确。
            // next() 返回 [0, n) 的逻辑索引，运行时再去 MetaManager::GetNodeKeys(0, node_id) 取真实 key，
            // 真实 key 自然定位到正确的 page（B+ 树/IndexCache 解析）。
            (void)page_num_per_node;  // 保留参数兼容老接口
            zip_fans.reserve(ComputeNodeCount);
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                std::vector<ZipFanGen> zipfan_vec;
                uint64_t zipf_seed = 2 * GetCPUCycle() * (int)(ramdom_string(20)[0] % ComputeNodeCount);
                uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
                uint64_t node_key_cnt = (i < (int)node_key_counts.size()) ? (uint64_t)node_key_counts[i] : 0;
                if (node_key_cnt == 0) node_key_cnt = 1;  // 防御：避免 ZipFanGen 断言 n>0
                zipfan_vec.emplace_back(ZipFanGen(node_key_cnt , zipf_theta , zipf_seed & zipf_seed_mask));
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

        // SYSTEM_MODE == 4：根据本事务的热点偏斜度，决定走 2PC 还是 Lazy 提交
        dtx->DecideCommitMode();

        for (int i = 0 ; i < 10 ; i++){
            if (rw_flags[i]){
                auto rw_user_id = std::make_shared<DataItem>(0);
                dtx->AddToReadWriteSet(rw_user_id , keys[i]);
            }else {
                auto ro_user_id = std::make_shared<DataItem>(0);
                dtx->AddToReadOnlySet(ro_user_id , keys[i]);
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
    
    // 生成 10 个 key，生成时需要注意两个规则
    // 1. 是否是热点数据(ZipFian 不需要这个规则)
    // 2. 是否是跨分区访问数据
    void generate_ten_keys(std::vector<itemkey_t> &keys , uint64_t *seed , bool is_partitioned , DTX *dtx){
        int belonged_node_id;
        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            belonged_node_id = dtx->compute_server->get_node()->ts_cnt;
        }else {
            belonged_node_id = dtx->compute_server->getNodeID();
        }
       
        page_id_t page_id;

        int partition_size = dtx->compute_server->get_node()->getMetaManager()->GetPartitionSizePerTable(0);
        int now_page_num = dtx->compute_server->get_node()->getMetaManager()->GetTablePageNum(0);
        int par_cnt = now_page_num / partition_size + 1;
        int node_page_num;

        for (int i = 0 ; i < 10 ; i++){
            int target_node_id;
            if (ComputeNodeCount == 1){
                // 如果只有一个节点，那就无所谓是否分区了
                target_node_id = dtx->compute_server->getNodeID();
            }else if (is_partitioned){
                target_node_id = FastRand(seed) % ComputeNodeCount;
            }else {
                target_node_id = belonged_node_id;
            }
            int account_cnt_per_page = PAGE_SIZE / tuple_size;
            // slot_in_page：仅在 access_pattern==1 (zipfian) 时由 zipfian 输出决定页内偏移；
            // 其他模式下回退为页内随机
            int slot_in_page = -1;
            if (access_pattern == 1){
                auto* mm = dtx->compute_server->get_node()->getMetaManager();
                const auto& node_keys = mm->GetNodeKeys(0, target_node_id);
                assert(!node_keys.empty());
                uint64_t idx = zip_fans[target_node_id][0].next();
                if (idx >= (uint64_t)node_keys.size()) idx %= (uint64_t)node_keys.size();
                keys[i] = node_keys[idx];

                // 接下来，需要去判断下这个选择的 key 是否是热点 key
                // 把 zipfian 访问索引落在前 HOT_KEY_TOP_N 个的视为热点 key（来自 compute_node_config.json）
                {
                    uint64_t total_keys = node_keys.size();
                    uint64_t num_hot_keys = (uint64_t)HOT_KEY_TOP_N;
                    if (num_hot_keys > total_keys) num_hot_keys = total_keys;
                    dtx->NoteKeyAccess(idx < num_hot_keys);
                }
                continue;
            }
            // 不用 zipfian，但是页面的 key 是随机分布的
            if (access_pattern == 0 && random_generate){
                // Uniform + random_generate：page 反映射不再可靠（key 在页面间随机分布，
                // 旧的 partition 映射可能落到 PageCache 中无 key 的物理页，触发 -1 断言）。
                // 同样改用「节点 key 列表」做热/冷拆分：
                //   - 节点本地的热 key 数量按整体热点比例换算：node_keys.size() * hot_record_cnt / record_count
                //   - 前 num_hot_keys 个为热 key，其余为冷 key（与按 page 排序的旧语义一致）
                auto* mm = dtx->compute_server->get_node()->getMetaManager();
                const auto& node_keys = mm->GetNodeKeys(0, target_node_id);
                assert(!node_keys.empty());
                uint64_t total_keys = node_keys.size();
                uint64_t num_hot_keys = (record_count > 0)
                    ? (uint64_t)((double)total_keys * ((double)hot_record_cnt / (double)record_count))
                    : 0;
                if (num_hot_keys == 0) num_hot_keys = 1;          // 防御：避免 % 0
                if (num_hot_keys > total_keys) num_hot_keys = total_keys;
                uint64_t idx;
                bool is_hot;
                if (FastRand(seed) % 100 < (uint64_t)tx_hot_rate){
                    idx = FastRand(seed) % num_hot_keys;
                    is_hot = true;
                } else {
                    uint64_t cold_cnt = total_keys - num_hot_keys;
                    if (cold_cnt == 0){
                        idx = FastRand(seed) % num_hot_keys;
                        is_hot = true;
                    } else {
                        idx = num_hot_keys + (FastRand(seed) % cold_cnt);
                        is_hot = false;
                    }
                }
                keys[i] = node_keys[idx];
                dtx->NoteKeyAccess(is_hot);
                continue;
            }
            assert(access_pattern == 0);
            bool seq_is_hot = false;
            // 页面的 key 是顺序排列的，且不用 zipfian
            {
                // 根据 is_partition 和 TX_HOT 以及热点事务的比例来生成一个 page_id
                node_page_num = dtx->compute_server->get_node()->getMetaManager()->GetPageNumPerNode(target_node_id , 0 , ComputeNodeCount);
                int num_hot_this_node = (int)((double)node_page_num * ((double)hot_record_cnt / (double)record_count));
                if (num_hot_this_node <= 0) num_hot_this_node = 1;          // 防御
                if (num_hot_this_node >= node_page_num) num_hot_this_node = node_page_num;
                if (FastRand(seed) % 100 < tx_hot_rate){
                    // 热点事务，需要访问热点页面
                    page_id = FastRand(seed) % num_hot_this_node;
                    seq_is_hot = true;
                }else {
                    int cold_pages = node_page_num - num_hot_this_node;
                    if (cold_pages <= 0){
                        page_id = FastRand(seed) % num_hot_this_node;
                        seq_is_hot = true;
                    } else {
                        page_id = (FastRand(seed) % cold_pages) + num_hot_this_node;
                        seq_is_hot = false;
                    }
                }
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

            // 顺序生成模式下 key = (page_id-1)*account_cnt_per_page + slot_in_page
            int slot = (slot_in_page >= 0) ? slot_in_page : (int)(FastRand(seed) % account_cnt_per_page);
            keys[i] = (page_id - 1) * account_cnt_per_page + slot;
            dtx->NoteKeyAccess(seq_is_hot);
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
    double zipf_theta;
    bool random_generate;       // true: key 在页面间随机分布；false: key 顺序生成（页面 0 存 0..N-1, 页面 1 存 N..2N-1）

public:
    bool getRandomGenerate() const { return random_generate; }

    int read_op_per_txn;        // 单个事务要做几次读操作，这个值是根据 read_percent 计算的
    int write_op_per_txn;       // 同上
    std::vector<bool> rw_flags; // 假如 read_op_per_txn = 9 , write_op_per_txn = 1，那这个数组的值就是 0000000001

    int tuple_size;

    // zip_fans[i][j]：第 i 个节点的第 j 个表的 zipfans 账户生成
    std::vector<std::vector<ZipFanGen>> zip_fans;

    std::atomic<int> now_account{0};

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
