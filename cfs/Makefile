.PHONY: all
all: sched

.PHONY: deps
deps:
	sudo apt-get install -y gcc-10 g++-10 python3-pip cmake
	sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10

.PHONY: lib
lib:
	make -C lib

.PHONY: nosched
nosched:
	cmake -B build -DCMAKE_BUILD_TYPE=Release
	cmake --build build --parallel `nproc`

.PHONY: sched
sched:
	cmake -B build -DCMAKE_BUILD_TYPE=Release -DCFS_SCHED_TYPE=DO_SCHED -DCFS_TRACE_ENABLE=1
	cmake --build build --parallel `nproc`


.PHONY: clean
clean:
	rm -rf build
