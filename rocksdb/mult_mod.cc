// Parts of the code are derived from examples/simple_example.cc
// Does open, read and write of a rocksdb instance

#include <cstdio>
#include <string>
#include <iostream>
#include <thread>

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

void db1(const std::string& kDBPath);
void read_only_db(const std::string& kDBPath, const std::string& kSecondaryPath);
int maindb_create(std::string kDBPath);


int main()
{
    std::string kDBPath_main_db = "/tmp/rocksdb_multithreading";
    std::string kSecondaryPath_read_db1 = "/tmp/rocksdb_multi_sec_db1/";
    std::string kSecondaryPath_read_db2 = "/tmp/rocksdb_multi_sec_db2/";

    maindb_create(kDBPath_main_db);

    std::thread thread1(db1, std::cref(kDBPath_main_db));
    std::thread thread2(read_only_db, std::cref(kDBPath_main_db), std::cref(kSecondaryPath_read_db1));
    std::thread thread3(read_only_db, std::cref(kDBPath_main_db), std::cref(kSecondaryPath_read_db2));

    thread1.join();
    thread2.join();
    thread3.join();

    return 0;
}


int maindb_create(std::string kDBPath) {
    DB *db;

    std::cout << db << "\n" << *db;
    Options options;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    std::cout << "MT:\tCreated table at " << kDBPath << "\n";
    assert(s.ok());

    // Write 1st pair
    s = db->Put(WriteOptions(), "game2", "valorant");
    assert(s.ok());
    std::string value;

    return 0;
    // Close db pointer and try for db1 writes...
}

void db1(const std::string& kDBPath) {
    DB *db;
    Options options;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    // create the DB if it's not already present
    options.create_if_missing = true;

    // // open DB
    Status s = DB::Open(options, kDBPath, &db);

    // Write 1st pair
    s = db->Put(WriteOptions(), "game3", "pubg");
    assert(s.ok());
    std::string value;

    // Read 1st pair
    s = db->Get(ReadOptions(), "game1", &value);
    assert(s.ok());
    std::cout << "MT:\tkey: game1\t value: " << value << "\n";
    assert(value == "valorant");

    // Write 2nd pair
    s = db->Put(WriteOptions(), "game2", "csgo");
    assert(s.ok());

    // Read 2nd pair
    s = db->Get(ReadOptions(), "game2", &value);
    assert(s.ok());
    std::cout << "MT:\tkey: game2\t value: " << value << "\n";
    assert(value == "csgo");

    std::cout << "MT:\tIterating over the entire db now!\n";
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout <<"MT:\t" << it->key().ToString() << ": " << it->value().ToString() << "\n";
    }

    assert(it->status().ok()); // Check for any errors found during the scan
    assert(db);

    delete it;
    delete db;
}

void read_only_db(const std::string& kDBPath, const std::string& kSecondaryPath) {
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

    // Read operations
    s2 = db_secondary->Get(ReadOptions(), "game2", &value);
    std::cout << "RT:\tkey: game2\t value: " << value << "\n";
}
