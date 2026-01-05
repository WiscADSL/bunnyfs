.PHONY: default
default: release

TARGETS := bench tests tree prep

.PHONY: release debug
release debug:
	mkdir -p build-$@
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ --target $(TARGETS) --parallel `nproc`

.PHONY: clean
clean:
	rm -rf build-*
