#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

LOG="${ROOT}/results/local-benchmarks/m1max-run.log"
mkdir -p "${ROOT}/results/local-benchmarks"

exec > >(tee -a "${LOG}") 2>&1

echo "=== $(date -Iseconds) m1max benchmark run start ==="

git fetch origin

run_branch() {
  local branch="$1"
  shift
  echo ""
  echo "=== $(date -Iseconds) checkout ${branch} ==="
  git checkout "${branch}"
  git reset --hard "origin/${branch}"
  echo "=== $(date -Iseconds) run benchmarks on ${branch} ==="
  "$@"
  echo "=== $(date -Iseconds) done ${branch} ==="
}

run_branch add-twist-testing-benchmarks \
  ./scripts/run_apple_m4_benchmarks.sh --mode comparison \
  --out-dir results/local-benchmarks/baseline-m1max-3s \
  --build-dir build-benchmarks-m1max --jobs 10 --min-time 3.0 --warmups 3.0

run_branch better_exceptions-benchmarks \
  ./scripts/run_apple_m4_benchmarks.sh --mode oox-only \
  --out-dir results/local-benchmarks/better_exceptions-m1max-3s \
  --build-dir build-benchmarks-m1max --jobs 10 --min-time 3.0 --warmups 3.0

run_branch fully-pay-as-you-go-benchmarks \
  ./scripts/run_apple_m4_benchmarks.sh --mode oox-only \
  --out-dir results/local-benchmarks/fully-pay-as-you-go-m1max-3s \
  --build-dir build-benchmarks-m1max --jobs 10 --min-time 3.0 --warmups 3.0

echo "=== $(date -Iseconds) all m1max benchmark runs finished ==="
