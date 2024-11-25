#!/bin/bash

DB_DIR="/mnt/nvme2/leveldb_1023"
NUM=10000
CACHE_SIZE=$((1 * 1024 * 1024 * 1024))
WORKLOADS=(10000 40000 50000)

for workload in "${WORKLOADS[@]}"; do

  sudo ./db_bench --db=$DB_DIR \
  --benchmarks=stats,readrandom,stats \
  --value_size=1024 --num=$NUM --bloom_bits=10 \
  --cache_size=$CACHE_SIZE --write_buffer_size=67108864 \
  --max_file_size=67108864 --use_existing_db=1 \
  --open_files=10000 \

  sudo ./db_bench --db=$DB_DIR \
  --benchmarks=stats,readwhilewriting,stats \
  --value_size=1024 --num=$NUM --bloom_bits=10 \
  --cache_size=$CACHE_SIZE --write_buffer_size=67108864 \
  --max_file_size=67108864 --use_existing_db=1 \
  --open_files=10000 \
  --write_per_sec_num=$workload \
  --cache_monitor=1 --report_interval_seconds=1 \
   > "../readwhilewriting/result/$workload.txt"

  mv ../report.csv "../readwhilewriting/report/$workload.csv"
  sudo mv /mnt/nvme2/leveldb_1023/LOG_compaction "../readwhilewriting/compaction/$workload.txt"

done

mv nohup.out "../readwhilewriting/nohup.out" 