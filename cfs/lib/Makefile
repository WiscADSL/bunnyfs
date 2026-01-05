FOLLY_INSTALLED = $(shell pkg-config --exists libfolly && echo 1 || echo 0)

.PHONY: all
all: folly fio spdk tbb config4cpp nanolog

.PHONY: folly
folly:
ifeq ($(FOLLY_INSTALLED), 1)
	@echo "Folly is already installed"
else
	@echo "Installing Folly"
	sh ../tools/folly_install.sh
endif

.PHONY: fio
fio:
	[ -d fio ] || git clone https://github.com/axboe/fio.git
	cd fio && make -j

.PHONY: spdk
spdk/config.h:
	sudo spdk/scripts/pkgdep.sh
	cd spdk && ./configure --with-fio=$(shell pwd)/fio
spdk: spdk/config.h fio
	make -C spdk -j
	make -C spdk -f Makefile.sharedlib -j

.PHONY: tbb
tbb:
	make -C tbb -j
	cd tbb/build && [ -d tbb_build_release ] || ln -s linux_*_release tbb_build_release

.PHONY: config4cpp
config4cpp:
	make -C config4cpp -j

.PHONY: nanolog
nanolog:
	make -C Nanolog/runtime -j
