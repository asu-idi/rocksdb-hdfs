# Sample code for rocksdb that has been connected with hdfs

## Usage:
1. the make_config.mk file should be in the parent directory of this folder (will be added in the future). The make_config.mk file is created when you make rocksdb.
2. Can use: 
    - `make testhdfs` to put data
    - `make rhdfs_readonly` to create a read only database and get data from it.
