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

std::string kDBPath = "/tmp/rocksdb_simple_example";

int main() {

  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

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

  // do stuff in a batch
  // {
  //   WriteBatch batch;
  //   batch.Delete("key1");
  //   batch.Put("key2", "value");
  //   s = db->Write(WriteOptions(), &batch);
  // }

  std::cout << "Iterating over the entire db now!\n";
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
  }
  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;

  delete db;

  return 0;
}
