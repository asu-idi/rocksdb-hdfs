// Parts of the code are derived from examples/simple_example.cc
// Does open, read and write of a rocksdb instance

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

int db1(std::string kDBPath) {
    DB *db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    std::cout << "Master Table " << "\n\n";

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

    assert(db);
    delete db;

    return 0;
}

int read_only_db(std::string kDBPath, std::string kSecondaryPath) {
    Options options2;
    options2.max_open_files = -1;
    std::string value;

    // Secondary instance needs its own directory to store info logs (LOG)
    DB *db_secondary = nullptr;
    Status s2 = DB::OpenAsSecondary(options2, kDBPath, kSecondaryPath, &db_secondary);
    assert(!s2.ok() || db_secondary);
    // Let secondary **try** to catch up with primary
    s2 = db_secondary->TryCatchUpWithPrimary();
    assert(s2.ok());

    std::cout << "\n\n" << "Read only Table" << "\n\n";

    // Read operations
    s2 = db_secondary->Get(ReadOptions(), "game2", &value);
    std::cout << "key: game2\t value: " << value << "\n";

    // try to write
    // s2 = db_secondary->Put(WriteOptions(), "game3", "csjnjngo");
    // assert(s2.ok());

    // Read operations
    // s2 = db_secondary->Get(ReadOptions(), "game3", &value);
    // assert(s2.ok());

    // std::cout << "key: game3\t value: " << value << "\n";

    return 0;
}

// Keeping this global

int main()
{
    std::string kDBPath_main_db = "/tmp/rocksdb_with_readonly";
    std::string kSecondaryPath_read_db1 = "/tmp/rocksdb_secondary1/";
    std::string kSecondaryPath_read_db2 = "/tmp/rocksdb_secondary2/";

    db1(kDBPath_main_db);
    read_only_db(kDBPath_main_db, kSecondaryPath_read_db1);
    read_only_db(kDBPath_main_db, kSecondaryPath_read_db2);

    return 0;
}