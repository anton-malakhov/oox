#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run_all_apple_m4_benchmark_branches.sh [options] [extra run_benchmarks.py args...]

Runs the full Apple M4 benchmark matrix:

  OOX implementations (one per branch):
    - add-twist-testing-benchmarks      -> taskbench/oox-twist.csv
    - better_exceptions-benchmarks      -> taskbench/oox-better-exc.csv
    - fully-pay-as-you-go-benchmarks    -> taskbench/oox-pay-as-you-go.csv

  Baseline runners (on every branch build, or once with BASELINES_ONCE=1):
    - tbb-flow   -> taskbench/tbb-flow.csv
    - taskflow   -> taskbench/taskflow.csv
    - openmp     -> taskbench/openmp.csv

Uses detached git worktrees so your main checkout is not switched.
Copies scripts/run_benchmarks.py and benchmarks/CMakeLists.txt from this
checkout into each worktree before building.

Environment:
  OUT_ROOT           Results root (default: results/local-benchmarks)
  WORKTREE_ROOT      Worktree parent dir (default: ../oox-apple-m4-benchmark-worktrees)
  JOBS               Build parallelism (default: 16)
  REUSE_WORKTREES    Set to 1 to keep existing worktrees
  BASELINES_ONCE     Set to 1 to run tbb-flow/taskflow/openmp only on the first branch
  ALL_RUNNERS        Override runner list (default: oox,tbb-flow,taskflow,openmp)
  MIN_TIME           Min benchmark seconds (default: 3.0)
  WARMUPS            Warmup seconds (default: 3.0)

Examples:
  ./scripts/run_all_apple_m4_benchmark_branches.sh
  BASELINES_ONCE=1 ./scripts/run_all_apple_m4_benchmark_branches.sh
  REUSE_WORKTREES=1 ./scripts/run_all_apple_m4_benchmark_branches.sh --skip-configure
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

OUT_ROOT="${OUT_ROOT:-results/local-benchmarks}"
WORKTREE_ROOT="${WORKTREE_ROOT:-../oox-apple-m4-benchmark-worktrees}"
JOBS="${JOBS:-16}"
REUSE_WORKTREES="${REUSE_WORKTREES:-0}"
BASELINES_ONCE="${BASELINES_ONCE:-0}"
ALL_RUNNERS="${ALL_RUNNERS:-oox,tbb-flow,taskflow,openmp}"
OOX_RUNNERS="oox"
BASELINE_RUNNERS="tbb-flow,taskflow,openmp"
MIN_TIME="${MIN_TIME:-3.0}"
WARMUPS="${WARMUPS:-3.0}"
EXTRA_ARGS=("$@")

RUNNER_OVERLAY=(
  scripts/run_benchmarks.py
  benchmarks/CMakeLists.txt
)

# branch_name:oox_label
branches=(
  "add-twist-testing-benchmarks:twist"
  "better_exceptions-benchmarks:better-exc"
  "fully-pay-as-you-go-benchmarks:pay-as-you-go"
)

resolve_branch_commit() {
  local branch="$1"
  local ref
  for ref in "origin/${branch}" "${branch}"; do
    if git -C "${ROOT}" rev-parse --verify --quiet "${ref}^{commit}" >/dev/null 2>&1; then
      git -C "${ROOT}" rev-parse "${ref}^{commit}"
      return
    fi
  done
  echo "Cannot find local or origin ref for ${branch}" >&2
  exit 1
}

ensure_worktree_at() {
  local worktree="$1"
  local commit="$2"
  git -C "${worktree}" checkout --detach "${commit}"
  git -C "${worktree}" submodule update --init --recursive
}

overlay_runner_files() {
  local worktree="$1"
  local rel
  for rel in "${RUNNER_OVERLAY[@]}"; do
    if [[ ! -f "${ROOT}/${rel}" ]]; then
      echo "Missing runner overlay file in current checkout: ${ROOT}/${rel}" >&2
      exit 1
    fi
    mkdir -p "${worktree}/$(dirname "${rel}")"
    cp "${ROOT}/${rel}" "${worktree}/${rel}"
  done
}

run_branch() {
  local branch="$1"
  local oox_label="$2"
  local runners="$3"
  local worktree
  local build_dir
  local out_dir
  local commit

  worktree="${WORKTREE_ROOT}/${branch}"
  if [[ "${worktree}" != /* ]]; then
    worktree="$(cd "${ROOT}" && cd "${worktree}" && pwd)"
  fi
  build_dir="${worktree}/build-benchmarks-apple-m4"
  out_dir="${ROOT}/${OUT_ROOT}/${branch}-3s"
  if [[ "${out_dir}" != /* ]]; then
    out_dir="$(cd "${ROOT}" && cd "${out_dir}" && pwd)"
  fi
  commit="$(resolve_branch_commit "${branch}")"

  echo ""
  echo "========================================"
  echo "Branch: ${branch}"
  echo "OOX label: ${oox_label}"
  echo "Runners: ${runners}"
  echo "Output: ${out_dir}"
  echo "========================================"

  if [[ -e "${worktree}/.git" && "${REUSE_WORKTREES}" -eq 0 ]]; then
    git -C "${ROOT}" worktree remove "${worktree}" --force
  fi

  if [[ ! -e "${worktree}/.git" ]]; then
    git -C "${ROOT}" worktree add --detach "${worktree}" "${commit}"
    ensure_worktree_at "${worktree}" "${commit}"
  else
    echo "Syncing worktree to ${commit}"
    ensure_worktree_at "${worktree}" "${commit}"
  fi

  overlay_runner_files "${worktree}"

  rm -rf "${build_dir}"

  common_args=(
    --mode comparison
    --comparison-runners "${runners}"
    --oox-label "${oox_label}"
    --min-time "${MIN_TIME}"
    --warmups "${WARMUPS}"
    --skip-ctest
    --build-dir "${build_dir}"
    --out-dir "${out_dir}"
    --jobs "${JOBS}"
  )
  if ((${#EXTRA_ARGS[@]} > 0)); then
    common_args+=("${EXTRA_ARGS[@]}")
  fi

  if [[ -x "${worktree}/scripts/run_apple_m4_benchmarks.sh" ]]; then
    "${worktree}/scripts/run_apple_m4_benchmarks.sh" "${common_args[@]}"
  else
    python3 "${worktree}/scripts/run_benchmarks.py" --profile apple-m4 "${common_args[@]}"
  fi
}

if [[ "${WORKTREE_ROOT}" != /* ]]; then
  WORKTREE_ROOT="$(cd "${ROOT}" && cd "${WORKTREE_ROOT}" && pwd)"
fi
mkdir -p "${OUT_ROOT}" "${WORKTREE_ROOT}"

branch_index=0
for entry in "${branches[@]}"; do
  branch="${entry%%:*}"
  oox_label="${entry##*:}"
  branch_index=$((branch_index + 1))

  if [[ "${BASELINES_ONCE}" -eq 1 && "${branch_index}" -gt 1 ]]; then
    runners="${OOX_RUNNERS}"
  else
    runners="${ALL_RUNNERS}"
  fi

  run_branch "${branch}" "${oox_label}" "${runners}"
done

if [[ -f "${ROOT}/scripts/compare_local_benchmark_branches.py" ]]; then
  echo ""
  echo "=== Building consolidated comparison table ==="
  python3 "${ROOT}/scripts/compare_local_benchmark_branches.py" \
    --results-root "${ROOT}/${OUT_ROOT}" \
    --suffix=-3s || true
fi

echo ""
echo "Done."
echo "Per-branch results: ${ROOT}/${OUT_ROOT}/<branch>-3s/"
echo "Consolidated report: ${ROOT}/${OUT_ROOT}/full_comparison.md (if compare script succeeded)"
echo "Worktrees: ${WORKTREE_ROOT} (REUSE_WORKTREES=1 to reuse)"
