#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

#include "env_hdfs.h"
#include "hdfs.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

std::string kDBPath = "rdb_hdfs";
std::string kDBPathSecondary = "rdb_readonly_hdfs";

int main()
{
    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv("hdfs://localhost:9000/rdb", &hdfs);

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = true;
    options.max_open_files = -1;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    

    // Open DB
    DB *db_secondary = nullptr;
    Status s2 = DB::OpenAsSecondary(options, kDBPath, kDBPathSecondary, &db_secondary);
    assert(!s2.ok() || db_secondary);
 
    // Let secondary **try** to catch up with primary
    s2 = db_secondary->TryCatchUpWithPrimary();
    assert(s2.ok());

    std::cout << "Iterating over the entire db now!\n";
    rocksdb::Iterator *it = db_secondary->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
    }
    assert(it->status().ok());

    s2 = db_secondary->Close();

    // delete it;
    // delete db;

    return 0;
}
