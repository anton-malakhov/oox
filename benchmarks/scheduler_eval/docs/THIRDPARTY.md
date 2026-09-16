# Third-party notices

## PASL graph topologies and parameter formulas

The `Pasl*` branches in `workloads/graph_workloads.cpp` and the parameter formulas in
`tools/paper_graphs.py` adapt `graph/include/graphgenerators.hpp` and
`graph/bench/graph.ml` from PASL commit
`d3ed9488cea5a8d35b9a86b4408e0f6f9211413b`.
The SC15 trunk-first and RMat formulas additionally use
`d2147d5986866d6060b6dee562fa65df432f90b7` (`new-sc15-graph`), also Apache-2.0.

Copyright (c) 2014 Umut Acar, Arthur Chargueraud, and Michael Rainey.
All rights reserved.

Licensed under Apache-2.0, as distributed in this repository's root license.
OOX modifications replace graph containers with CSR construction, add bounded
small cases, omit vertex permutation in native variants, and add Python
command/provenance handling. The original generator path preserves its own
permutation. No PASL runtime source is vendored here.

## Deepsea SPTL granularity estimator

`workloads/granularity_control.{h,cpp}` adapts the estimator rule from
`deepsea-inria/sptl` commit `911bc7af7c658020138a08d4923224332b08a27f`.

MIT License

Copyright (c) 2017 Deepsea

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
