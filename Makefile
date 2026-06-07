# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.NOTPARALLEL:
export SHELL=bash

NPROC := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)

NUMACTL := $(shell command -v numactl >/dev/null 2>&1 && echo "numactl -N 0")

.PHONY: all
all: release test bench_fib

release:
	cmake -B build -S . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOOX_ALLOCATOR=tbb && cmake --build build -j$(NPROC)

debug:
	cmake -B build_debug -S . -DCMAKE_BUILD_TYPE=Debug && cmake --build build_debug -j$(NPROC)

clean:
	rm -rf build build_debug

test:
	cd build/; ctest

install:
	cmake --build build --target install

bench_loops:
	@echo ----------------------------------------------------------------------------
	@echo -e "Loop_mode\tBench\tSize            \tIters\tTime\tCPU\tUnit"
	@echo ----------------------------------------------------------------------------
	@for x in $(shell ls -1 build/benchmarks/bench_loops_*) ; do $(NUMACTL) $$x --benchmark_format=csv 2>/dev/null | grep /real_time | tr /, '\t'; done

bench_fib:
	$(NUMACTL) build/benchmarks/bench_fib_TBB.exe

bench: bench_fib bench_loops
