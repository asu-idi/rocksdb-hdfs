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

std::string kDBPath_1 = "/tmp/rocksdb_double_1";
std::string kDBPath_2 = "/tmp/rocksdb_double_2";

int main() {

  DB* db1;
  DB* db2;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath_1, &db1);
  std::cout << "Created table at " << kDBPath_1 << "\n";
  assert(s.ok());

  s = DB::Open(options, kDBPath_2, &db2);
  std::cout << "Created table at " << kDBPath_1 << "\n";
  assert(s.ok());

  // Write 1st pair
  s = db1->Put(WriteOptions(), "game1-db1", "valorant");
  assert(s.ok());
  std::string value;

  s = db2->Put(WriteOptions(), "game1-db2", "valorant");
  assert(s.ok());


  // Read 1st pair
  s = db1->Get(ReadOptions(), "game1-db1", &value);
  assert(s.ok());
  std::cout << "key: game1-db1\t value: " << value << "\n";
  assert(value == "valorant");

  s = db2->Get(ReadOptions(), "game1-db2", &value);
  assert(s.ok());
  std::cout << "key: game1-db2\t value: " << value << "\n";
  assert(value == "valorant");


  // Write 2nd pair
  s = db1->Put(WriteOptions(), "game2-db1", "csgo");
  assert(s.ok());

  // Read 2nd pair
  s = db1->Get(ReadOptions(), "game2-db1", &value);
  assert(s.ok());
  std::cout << "key: game2\t value: " << value << "\n";
  assert(value == "csgo");

  // do stuff in a batch
  // {
  //   WriteBatch batch;
  //   batch.Delete("key1");
  //   batch.Put("key2", "value");
  //   s = db->Write(WriteOptions(), &batch);
  // }

  std::cout << "\nIterating over the entire db1 now!\n";
  rocksdb::Iterator* it = db1->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
  }
  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;

  std::cout << "\nIterating over the entire db2 now!\n";
  rocksdb::Iterator* it2 = db2->NewIterator(rocksdb::ReadOptions());
  for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
    std::cout << it2->key().ToString() << ": " << it2->value().ToString() << "\n";
  }
  assert(it2->status().ok()); // Check for any errors found during the scan
  delete it2;

  delete db1;
  delete db2;

  return 0;
}
