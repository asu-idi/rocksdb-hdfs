#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"

#include "env_hdfs.h"
#include "hdfs.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

std::string kDBPath = "/tmp";

int main()
{
    DB *db;

    Options options;
    // std::unique_ptr<rocksdb::Env> hdfs;
    // NewHdfsEnv("hdfs://localhost:9000", &hdfs);
    // options.env = hdfs.get();

    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv("hdfs://localhost:9000/tmp", &hdfs);
    options.env = hdfs.get();

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.create_if_missing = true;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    
    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    std::cout << "Created table at " << kDBPath << "\n";
    assert(s.ok());

    // Write 1st pair
    s = db->Put(WriteOptions(), "game1", "valorant");
    assert(s.ok());
    std::string value;

    // Read 1st pair
    s = db->Get(ReadOptions(), "game1", &value);
    assert(s.ok());
    std::cout << "key: game1\t value: " << value << "\n";
    assert(value == "valorant");

    // Write 2nd pair
    s = db->Put(WriteOptions(), "game2", "csgo");
    assert(s.ok());

    // Read 2nd pair
    s = db->Get(ReadOptions(), "game2", &value);
    assert(s.ok());
    std::cout << "key: game2\t value: " << value << "\n";
    assert(value == "csgo");

    std::cout << "Iterating over the entire db now!\n";
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
    }
    assert(it->status().ok()); // Check for any errors found during the scan

    delete it;
    delete db;

    return 0;
}
