#!/usr/bin/env bash
# Run inside the diagnostic container. Mount a source snapshot at /src (read-only)
# and a fresh output directory at /out. No source files are changed.
set -euo pipefail
mkdir -p /out/logs
exec > >(tee /out/logs/session.log) 2>&1
export BENCH_NUM_THREADS=${BENCH_NUM_THREADS:-8}
unset PARLAY_NUM_THREADS OMP_NUM_THREADS
{ date -Is; uname -a; lscpu; "${CXX:-c++}" --version; cmake --version;
  dpkg-query -W libtbb-dev;
  printf 'BENCH_NUM_THREADS=%s\nSANITIZE=%s\nBUILD_JOBS=%s\nREPEATS=%s\nTEST_TIMEOUT=%s\n' \
    "$BENCH_NUM_THREADS" "${SANITIZE:-none}" "${BUILD_JOBS:-4}" "${REPEATS:-20}" "${TEST_TIMEOUT:-180}";
  printf 'ordinary: calibration/default limit; final: calibration=0 resident_limit=%s\n' "$BENCH_NUM_THREADS";
} > /out/logs/environment.txt 2>&1
modes=(SERIAL_ELISION TBB_SIMPLE TBB_AUTO EIGEN_AUTO EIGEN_AUTO_RESIDENT EIGEN_STEALING RAPID_GROUP RAPID_AUTO)
targets=("${modes[@]/#/test_scheduler_eval_}")
targets+=(test_eval_dedup_spurious)
targets+=(test_eval_unpinned_TBB_SIMPLE test_eval_unpinned_TBB_AUTO)
options=()
if [[ -d /deps/googletest ]]; then options+=(-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/deps/googletest); fi
if [[ -d /deps/benchmark ]]; then options+=(-DFETCHCONTENT_SOURCE_DIR_BENCHMARK=/deps/benchmark); fi
if [[ -n ${SANITIZE:-} ]]; then options+=("-DOOX_SANITIZE=$SANITIZE"); fi
cmake -S /src -B /out/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DOOX_BUILD_TESTS=OFF -DOOX_BUILD_BENCHMARKS=ON -DOOX_BUILD_EXAMPLES=OFF \
  -DOOX_BUILD_TASKBENCH=OFF -DOOX_BUILD_SCHEDULER_EVALS=ON \
  -DOOX_ENABLE_EIGEN=ON -DOOX_ENABLE_TBB=ON -DOOX_ENABLE_TF=OFF \
  -DOOX_ENABLE_FOLLY=OFF -DOOX_ENABLE_OMP=OFF \
  -DOOX_SCHEDULER_EVAL_STATS=OFF -DOOX_SCHEDULER_EVAL_PAPI=OFF "${options[@]}"
cp /out/build/CMakeCache.txt /out/logs/
cmake --build /out/build --parallel "${BUILD_JOBS:-4}" --target "${targets[@]}"
ldd /out/build/benchmarks/scheduler_eval/test_scheduler_eval_TBB_AUTO > /out/logs/tbb-libraries.txt
failed=0
run_test() {
  local label=$1 pattern=$2 status=0
  ctest --test-dir /out/build -R "$pattern" -V -j1 --no-tests=error \
    --repeat "until-fail:${REPEATS:-20}" --timeout "${TEST_TIMEOUT:-180}" \
    --output-log "/out/logs/$label.log" || status=$?
  printf '%s exit=%s\n' "$label" "$status" | tee -a /out/logs/status.txt
  if ((status)); then failed=1; fi
}
unset OOX_RAPID_AUTOCALIBRATE OOX_RAPID_RESIDENT_LIMIT
run_test dedup_spurious '^scheduler_eval_dedup_spurious$'
run_test unpinned_tbb '^test_eval_unpinned_TBB_(SIMPLE|AUTO)$'
for mode in "${modes[@]}"; do run_test "$mode" "^test_scheduler_eval_${mode}$"; done
export OOX_RAPID_AUTOCALIBRATE=0 OOX_RAPID_RESIDENT_LIMIT=$BENCH_NUM_THREADS
run_test RAPID_AUTO_fixed "^test_scheduler_eval_RAPID_AUTO$"
exit "$failed"
