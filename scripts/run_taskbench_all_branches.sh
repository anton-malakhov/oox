#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${OOX_TASKBENCH_RUNNER_FROZEN:-}" ]]; then
  frozen_script="$(mktemp "/tmp/run_taskbench_all_branches.XXXXXX.sh")"
  cp "$0" "${frozen_script}"
  chmod +x "${frozen_script}"
  OOX_TASKBENCH_RUNNER_FROZEN=1 exec "${frozen_script}" "$@"
fi

branches=(
  "add-twist-testing-benchmarks"
  "better_exceptions-benchmarks"
  "fully-pay-as-you-go-benchmarks"
)

patterns="stencil,sweep,nearest,spread,random,fft,tree"
height="1000"
width="16,32,64"
default_min_time="3.0"
default_warmups="3.0"
quick_min_time="0.1"
quick_warmups="0.1"
stamp="$(date +%Y%m%d-%H%M%S)"
results_root="results/taskbench-all-branches/${stamp}"

selected_min_time="${default_min_time}"
selected_warmups="${default_warmups}"
for arg in "$@"; do
  if [[ "${arg}" == "--quick" ]]; then
    selected_min_time="${quick_min_time}"
    selected_warmups="${quick_warmups}"
    break
  fi
done

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree has tracked changes. Commit/stash before running this script." >&2
  exit 1
fi

orig_branch="$(git branch --show-current)"

restore_branch() {
  git checkout "${orig_branch}" >/dev/null 2>&1 || true
}
trap restore_branch EXIT

for branch in "${branches[@]}"; do
  echo
  echo "=== ${branch} ==="
  git checkout "${branch}"
  if [[ ! -f scripts/run_benchmarks.py ]]; then
    echo "Missing scripts/run_benchmarks.py on ${branch}" >&2
    exit 2
  fi
  branch_out_dir="${results_root}/${branch}"
  branch_build_dir="build-taskbench-${branch}"
  python3 scripts/run_benchmarks.py \
    --build-dir "${branch_build_dir}" \
    --out-dir "${branch_out_dir}" \
    --skip-ctest \
    --skip-big-graph-smoke \
    --height "${height}" \
    --width "${width}" \
    --patterns "${patterns}" \
    --min-time "${selected_min_time}" \
    --warmups "${selected_warmups}" \
    "$@"
done

echo
echo "Completed taskbench runs for all benchmark branches."
echo "Results root: ${results_root}"
