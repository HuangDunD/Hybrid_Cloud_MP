// Author: Hongyao Zhao
// Copyright (c) 2024

#include "smallbank/smallbank_txn.h"
#include "scheduler/coroutine.h"

/******************** The original logic (Transaction) start ********************/
bool SmallBankDTX::TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zipfans) {
  // 获取事务开始 ts
  dtx->TxBegin(tx_id);
  
  // 生成账号
  uint64_t acct_id_0, acct_id_1;
  if (zipfans == nullptr){
    // 非 zipfans 模式，在此模式下，冷热分区，每个热页面的访问概率是一样的，每个冷页面的访问概率也是一样的，可以在 smallbank_config.json 里面设置 TX_HOT 来控制冷热页面访问的比例
    // 第一个账号 acct_id_0，是 saving 表的
    smallbank_client->get_account(seed, &acct_id_0, dtx, is_partitioned, dtx->compute_server->get_node()->getNodeID() , 0);
    // 第二个账号，acct_id_1，checking 表
    do {
      smallbank_client->get_account(seed , &acct_id_1 , dtx , is_partitioned , dtx->compute_server->get_node()->get_node_id() , 1);
    }while(acct_id_0 == acct_id_1);
  }else{
    // zipfian 负载模式
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id_0 , (*zipfans)[target_node_id][1] , dtx , seed , 1 , target_node_id);
    do {
      smallbank_client->get_account(acct_id_1 , (*zipfans)[target_node_id][0] , dtx , seed , 0 , target_node_id);
    }while(acct_id_0 == acct_id_1);
  }

  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  auto sav_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable);
  dtx->AddToReadWriteSet(sav_obj_0, sav_key_0.item_key);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj_0, chk_key_0.item_key);

  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj_1, chk_key_1.item_key, true);
  
  // 执行事务
  if (!dtx->TxExe(yield)) return false;
  
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, saving 0 magic = " << sav_val_0->magic << " Original Magic = " << smallbank_savings_magic; 
    assert(false);
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  assert(sav_val_0->magic == smallbank_savings_magic);
  assert(chk_val_0->magic == smallbank_checking_magic);
  assert(chk_val_1->magic == smallbank_checking_magic);

  // 事务要做的事情
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;


  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fans) {
  dtx->TxBegin(tx_id);
  /* Transaction parameters */
  uint64_t acct_id;
  if (zip_fans == nullptr){
    // 只在 saving 表
    smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID() , 0);
  }else{
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id , (*zip_fans)[target_node_id][0] , dtx , seed , 0 , target_node_id);
  }

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable);
  dtx->AddToReadOnlySet(sav_obj, sav_key.item_key);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadOnlySet(chk_obj, chk_key.item_key);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  // smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    // LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }

  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;

  // return true;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fans) {
  dtx->TxBegin(tx_id);
    //  // LOG(INFO) << "TxDepositChecking" ;
  /* Transaction parameters */
  uint64_t acct_id;
  if (zip_fans == nullptr){
    // checking 表账号
    smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID() , 1);
  }else {
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id , (*zip_fans)[target_node_id][1] , dtx , seed , 1 , target_node_id);
  }
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj, chk_key.item_key, true);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock*/
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }

  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fans) {
  dtx->TxBegin(tx_id);
    //  // LOG(INFO) << "TxSendPayment" ;
  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  if (zip_fans == nullptr){
    // smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1,dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
    smallbank_client->get_account(seed , &acct_id_0 , dtx , is_partitioned , dtx->compute_server->getNodeID() , 1);
    do {
      smallbank_client->get_account(seed , &acct_id_1 , dtx , is_partitioned , dtx->compute_server->getNodeID() , 0);
    }while(acct_id_0 == acct_id_1);
  }else {
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id_0 , (*zip_fans)[target_node_id][1] , dtx , seed , 1 , target_node_id);
    do {
      smallbank_client->get_account(acct_id_1 , (*zip_fans)[target_node_id][0] , dtx , seed , 0 , target_node_id);
    }while(acct_id_0 == acct_id_1);
  }

  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj_0, chk_key_0.item_key);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj_1, chk_key_1.item_key, true);

  if (!dtx->TxExe(yield)) return false;

  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }

  if (chk_val_0->bal < amount) {
      // std::cout << "Insufficient balance cause Abort" ;
    dtx->TxAbortWorkLoad(yield);
    return true;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;

  // return true;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fans) {
  dtx->TxBegin(tx_id);

  uint64_t acct_id;
  if (zip_fans == nullptr){
    smallbank_client->get_account(seed, &acct_id,dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID() , 0);
  }else{
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id , (*zip_fans)[target_node_id][0] , dtx , seed , 0 , target_node_id);
  }

  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable);
  dtx->AddToReadWriteSet(sav_obj, sav_key.item_key, true);


  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fans) {
  dtx->TxBegin(tx_id);
    //  // LOG(INFO) << "TxWriteCheck" ;
  /* Transaction parameters */
  uint64_t acct_id;
  if (zip_fans == nullptr){
    smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID() , 0);
  }else{
    node_id_t target_node_id;
    if (is_partitioned){
        do {
            target_node_id = FastRand(seed) % ComputeNodeCount;
        }while(target_node_id == dtx->compute_server->getNodeID());
    } else {
        target_node_id = dtx->compute_server->getNodeID();
    }
    smallbank_client->get_account(acct_id , (*zip_fans)[target_node_id][0] , dtx , seed , 0 , target_node_id);
  }

  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable);
  dtx->AddToReadOnlySet(sav_obj, sav_key.item_key);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable);
  dtx->AddToReadWriteSet(chk_obj, chk_key.item_key, true);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  // LJWrongTag
  if (sav_val->magic != smallbank_savings_magic) {
    // LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    assert(false);
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
}

/******************** The business logic (Transaction) end ********************/
