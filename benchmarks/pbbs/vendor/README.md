# pbbsbench

The Problem Based Benchmark Suite (PBBS) is a collection of over 20
benchmarks defined in terms of their IO characteristics.  They are
designed to make it possible to compare different algorithms, or
implementations in different programming languages.  The emphasis is
on parallel algorithms.  Our default implementations are for
shared-memory multicore machines, although we would be delighted if
others implemented versions on GPUs or distributed machines and made
comparisons.

A list of the benchmarks can be found on
[here](https://cmuparlay.github.io/pbbsbench/benchmarks/index.html).

Information on the organization and on how to run PBBS can be found on
[here](https://cmuparlay.github.io/pbbsbench).

And here is a paper outlining the benchmarks:

[The problem-based benchmark suite (PBBS), V2](https://dl.acm.org/doi/10.1145/3503221.3508422)<br>
Daniel Anderson, Guy E. Blelloch, Laxman Dhulipala, Magdalen Dobson, and Yihan Sun<br>
ACM SIGPLAN Symposium on Principles & Practice of Parallel Programming (PPoPP), 2022

## How to run

It is proposed to use [run_benches.py](./run_benches.py) script.
* Currently 3 backends are supported: OMP, TBB and Eigen
    * OMP and Eigen are used similarly: provide `--omp` and/or `--eigen` options respectively
    * To use TBB, you'll need to have it built somewhere. Provide `--tbb-path path/to/tbb` and `--tbb` flags. It is expected that `path/to/tbb` has `include/` directory on the top level and `.so` somewhere within
* Sometimes checks work weirdly (and always long) so you might want to add `--nocheck` flag
* If you don't need the full measurement, use `--small` flag, then input sizes will be reduced. (Though, few benches will not work properly with small inputs)
* Script will generate a directory with an ugly name for logs for each backend. If you want to provide your own directory instead, add `--dir logs/dir`
    * There is a separate [script](./gather_stats.py) that turns logs into .jsons

Example:
```bash
./run_benches.py --tbb-path=$HOME/local --tbb --dir tmp_res/ --small
for file in tmp_res/*.txt ; do cat $file | ./gather_stats.py > $file.json ; done
```

After that in [graphs.ipynb](./graphs.ipynb) provide your logs directory (`tmp_res/` in example) and run all the cells. There are a few things you might need to tweak:
* `filter_exec` function: it's responsible for choosing backends which results will be displayed
* `target_execs` will highlight results for provided backends
* `base_exec` is a baseline backend. It also has to deal with highlighting
* `curr_target` will filter out all graphs where `curr_target` backend is absent
