// Author: Chunyue Huang
// Copyright (c) 2024

#include "smallbank_db.h"
#include "config.h"
#include "unistd.h"
#include "util/json_config.h"

void SmallBank::GenerateFriendGraph() {
  if (!UseWorkloadAffinity()) {
    user_friend_graph.clear();
    return;
  }
  if (affinity_graph_mode == "interleaved_hub") {
    generate_interleaved_hub_friend_graph(user_friend_graph,
                                          static_cast<int>(num_accounts_global),
                                          affinity_group_count,
                                          friend_degree_min,
                                          friend_degree_max,
                                          affinity_group_hubs,
                                          affinity_hub_weight);
  } else {
    generate_friend_simulate_graph(user_friend_graph,
                                   static_cast<int>(num_accounts_global),
                                   friend_degree_min,
                                   friend_degree_max);
  }
  std::cout << "Generated SmallBank affinity graph for " << num_accounts_global
            << " accounts, ratio=" << affinity_txn_ratio
            << ", mode=" << affinity_graph_mode
            << ", degree=[" << friend_degree_min << "," << friend_degree_max
            << "]\n";
}

bool SmallBank::TrySampleAffinityAccount(itemkey_t account_id,
                                         uint64_t* seed,
                                         itemkey_t* friend_account_id) const {
  if (!UseWorkloadAffinity() || friend_account_id == nullptr) {
    return false;
  }
  if (account_id >= user_friend_graph.size()) {
    return false;
  }
  const auto& friends = user_friend_graph[account_id];
  if (friends.empty()) {
    return false;
  }

  const double draw =
      static_cast<double>(FastRand(seed)) / static_cast<double>(UINT32_MAX);
  double cumulative = 0.0;
  for (const auto& entry : friends) {
    cumulative += entry.second;
    if (draw <= cumulative) {
      *friend_account_id = entry.first;
      return true;
    }
  }

  *friend_account_id = friends.back().first;
  return true;
}

void SmallBank::get_two_accounts(uint64_t* seed,
                                 uint64_t* acct_id_0,
                                 uint64_t* acct_id_1,
                                 const DTX* dtx,
                                 bool is_partitioned,
                                 node_id_t gen_node_id,
                                 table_id_t table_id_0,
                                 table_id_t table_id_1) {
  get_account(seed, acct_id_0, dtx, is_partitioned, gen_node_id, table_id_0);

  bool use_affinity = UseWorkloadAffinity() &&
                      (static_cast<double>(FastRand(seed)) /
                       static_cast<double>(UINT32_MAX)) < affinity_txn_ratio &&
                      TrySampleAffinityAccount(*acct_id_0, seed, acct_id_1);
  if (!use_affinity || *acct_id_0 == *acct_id_1) {
    do {
      get_account(seed, acct_id_1, dtx, is_partitioned, gen_node_id, table_id_1);
    } while (*acct_id_0 == *acct_id_1);
  }
}

void SmallBank::get_two_accounts(itemkey_t& acct_id_0,
                                 itemkey_t& acct_id_1,
                                 ZipFanGen* zip_fan_0,
                                 ZipFanGen* zip_fan_1,
                                 const DTX* dtx,
                                 uint64_t* seed,
                                 table_id_t table_id_0,
                                 table_id_t table_id_1,
                                 int target_node_id) {
  get_account(acct_id_0, zip_fan_0, dtx, seed, table_id_0, target_node_id);

  bool use_affinity = UseWorkloadAffinity() &&
                      (static_cast<double>(FastRand(seed)) /
                       static_cast<double>(UINT32_MAX)) < affinity_txn_ratio &&
                      TrySampleAffinityAccount(acct_id_0, seed, &acct_id_1);
  if (!use_affinity || acct_id_0 == acct_id_1) {
    do {
      get_account(acct_id_1, zip_fan_1, dtx, seed, table_id_1, target_node_id);
    } while (acct_id_0 == acct_id_1);
  }
}

void SmallBank::LoadTable(node_id_t node_id, node_id_t num_server) {
  // 根据存储节点的节点号来分配表的位置
  if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == node_id) {
    printf("Primary: Initializing SAVINGS table\n");
    PopulateSavingsTable();
  }
  if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == node_id) {
    printf("Primary: Initializing CHECKING table\n");
    PopulateCheckingTable();
  }
}

void SmallBank::VerifyData(){
    std::vector<std::string> table_names(2);
    table_names[0] = "smallbank_savings";
    table_names[1] = "smallbank_checking";

    std::unique_ptr<RmFileHandle> saving_table_file = rm_manager->open_file(table_names[0]);
    std::unique_ptr<RmFileHandle> checking_table_file = rm_manager->open_file(table_names[1]);

    std::cout << "Verifying SmallBank Data---\n";
    for (int account_id = 1 ; account_id < num_accounts_global ; account_id++){
      smallbank_savings_key_t sav_key;
      sav_key.acct_id = (uint64_t)account_id;
      sav_key.item_key = (uint64_t)account_id;
      smallbank_checking_key_t check_key;
      check_key.acct_id = (uint64_t)account_id;
      check_key.item_key = (uint64_t)account_id;

      Rid sav_rid , check_rid;
      bool sav_found = bl_indexes[0]->search(&sav_key.item_key , sav_rid);
      bool check_found = bl_indexes[1]->search(&check_key.item_key , check_rid);
      assert(sav_found);
      assert(check_found);

      std::unique_ptr<RmRecord> sav_record = saving_table_file->get_record(sav_rid , nullptr);
      std::unique_ptr<RmRecord> check_record = checking_table_file->get_record(check_rid , nullptr);
      assert(sav_record != nullptr);
      assert(check_record != nullptr);

      DataItem* sav_item = reinterpret_cast<DataItem*>(sav_record->value_);
      DataItem* check_item = reinterpret_cast<DataItem*>(check_record->value_);
      if (sav_item->value_size == sizeof(smallbank_savings_val_t)) {
          // assert(sav_item->key == sav_key.item_key);
          assert(sav_item->lock == 0);
      } else {
          assert(false);
      }
      
      if (check_item->value_size == sizeof(smallbank_checking_val_t)) {
          // assert(check_item->key == sav_key.item_key);
          assert(check_item->lock == 0);
      } else {
          assert(false);
      }
    }

    std::cout << "Vertfying SmallBank Val Success , Total Account = " << num_accounts_global << "\n";
}

// ljTAG：写入 key -> rid 映射
int SmallBank::LoadRecord(RmFileHandle* file_handle,
                          itemkey_t item_key,
                          void* val_ptr,
                          size_t val_size,
                          table_id_t table_id,
                          std::ofstream& indexfile
                          ) {
  /* Insert into Disk */
  DataItem item_to_be_inserted(table_id , (uint8_t*)val_ptr, val_size);
  char* item_char = (char*)malloc(item_to_be_inserted.GetSerializeSize());
  item_to_be_inserted.Serialize(item_char);
  Rid rid = file_handle->insert_record(item_key, item_char, nullptr);
  // record index
  indexfile << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;

  bl_indexes[table_id]->insert_entry(&item_key , rid);

  free(item_char);
  return 1;
}

void SmallBank::PopulateSavingsTable() {
  // 创建文件
  rm_manager->create_file(bench_name + "_savings", tuple_size);
  rm_manager->create_file(bench_name + "_savings_fsm" , tuple_size);

  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_savings");
  std::unique_ptr<RmFileHandle> table_file_fsm = rm_manager->open_file(bench_name + "_savings_fsm");

  std::ofstream indexfile;
  indexfile.open(bench_name + "_savings_index.txt");
  
  table_file->file_hdr_.record_size_ = tuple_size;
  table_file->file_hdr_.num_records_per_page_ = num_records_per_page;
  table_file->file_hdr_.bitmap_size_ = (num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
  rm_manager->get_diskmanager()->update_value(table_file->GetFd(), RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
  /* Populate the tables */
  // 插入用户数据，num_accounts_global 在 smallbank_config.json 里面配置
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Savings
    smallbank_savings_key_t savings_key;
    savings_key.acct_id = (uint64_t)acct_id;

    smallbank_savings_val_t savings_val;
    savings_val.magic = smallbank_savings_magic;
    savings_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), savings_key.item_key,
               (void*)&savings_val, sizeof(smallbank_savings_val_t),
                (table_id_t)SmallBankTableType::kSavingsTable,
                indexfile);
  }

  int fd1 = rm_manager->get_diskmanager()->open_file(bench_name + "_savings" + "_fsm");
  rm_manager->get_diskmanager()->update_value(fd1, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file_fsm->file_hdr_, sizeof(table_file_fsm->file_hdr_));
  int leftrecords = num_accounts_global % num_records_per_page;//最后一页的记录数
  fsm_trees[0]->update_page_space(num_pages, (num_records_per_page - leftrecords) * (tuple_size + sizeof(itemkey_t)));//更新最后一页的空间信息,free space为可插入的元组数量*（key+value）
  fsm_trees[0]->flush_all_pages();
  rm_manager->get_diskmanager()->close_file(fd1);

  // Flush BLink tree pages
  rm_manager->get_bufferPoolManager()->flush_all_pages(bl_indexes[0]->getFD());

  // head page页面需要直接写入磁盘不经过newPage()所以需要手动刷页
  int fd = rm_manager->get_diskmanager()->open_file(bench_name + "_savings");
  rm_manager->get_diskmanager()->update_value(fd, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
  rm_manager->close_file(table_file.get());
  indexfile.close();
}

void SmallBank::PopulateCheckingTable( ) {
  rm_manager->create_file(bench_name + "_checking", tuple_size);
  rm_manager->create_file(bench_name + "_checking_fsm" , tuple_size);
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_checking");
  std::unique_ptr<RmFileHandle> table_file_fsm = rm_manager->open_file(bench_name + "_checking_fsm");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_checking_index.txt");
  
  table_file->file_hdr_.record_size_ = tuple_size;
  table_file->file_hdr_.num_records_per_page_ = num_records_per_page;
  table_file->file_hdr_.bitmap_size_ = (num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
  rm_manager->get_diskmanager()->update_value(table_file->GetFd(), RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Checking
    smallbank_checking_key_t checking_key;
    checking_key.acct_id = (uint64_t)acct_id;

    smallbank_checking_val_t checking_val;
    checking_val.magic = smallbank_checking_magic;
    checking_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), checking_key.item_key,
               (void*)&checking_val, sizeof(smallbank_checking_val_t),
                (table_id_t)SmallBankTableType::kCheckingTable,
                indexfile);
  }

  int fd1 = rm_manager->get_diskmanager()->open_file(bench_name + "_checking" + "_fsm");
  rm_manager->get_diskmanager()->update_value(fd1, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file_fsm->file_hdr_, sizeof(table_file_fsm->file_hdr_));
  int leftrecords = num_accounts_global % num_records_per_page;//最后一页的记录数
  fsm_trees[1]->update_page_space(num_pages, (num_records_per_page - leftrecords) * (tuple_size + sizeof(itemkey_t)));//更新最后一页的空间信息,free space为可插入的元组数量*（key+value）
  fsm_trees[1]->flush_all_pages();
  rm_manager->get_diskmanager()->close_file(fd1);
  
  // Flush BLink tree pages
  int fsm_fd = bl_indexes[1]->getFD();
  rm_manager->get_bufferPoolManager()->flush_all_pages(fsm_fd);
  
  int fd = rm_manager->get_diskmanager()->open_file(bench_name + "_checking");
  rm_manager->get_diskmanager()->update_value(fd, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
  rm_manager->close_file(table_file.get());
}
