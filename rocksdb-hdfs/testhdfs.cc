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

int main()
{
    DB *db;

    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv("hdfs://localhost:9000/rdb", &hdfs);

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = true;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    
    // Open DB
    Status s = DB::Open(options, kDBPath, &db);
    std::cout << "Created table at " << kDBPath << "\n";

    // Write 1st pair
    s = db->Put(WriteOptions(), "game5", "apex");
    assert(s.ok());

    // Write 2nd pair
    Status s2 = db->Put(WriteOptions(), "game6", "pubg");
    std::cout << s2.ok();

    // Iterate over entire db
    std::cout << "Iterating over the entire db now!\n";
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
    }
    assert(it->status().ok());

    s = db->Close();
    assert(s.ok());

    return 0;
}
