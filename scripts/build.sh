#! /bin/bash
# exit when any command fails
set -e

mkdir -p cfs/build
cd cfs/build
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON "$@" ..
make clean
make -j $(nproc)

cd ../../
mkdir -p cfs_bench/build
cd cfs_bench/build
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
make clean
make -j $(nproc)

cd ../../bench
make clean
make -j $(nproc)
