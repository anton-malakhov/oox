# Eigen synchronization tests with Twist

This is a separate CMake project. It exercises the production Eigen queue and
scheduler headers through `OOX_EIGEN_TWIST`, which redirects their synchronization
operations to Twist. It does not define `HAVE_TWIST` or select OOX's separate
Twist task engine. No queue algorithm is reimplemented in these tests.

Use LLVM Clang 21 with a C++23 standard library. All fetched dependencies are
pinned in `CMakeLists.txt`, including the transitive Sure, sure-stack, function2,
fmt, backward-cpp, and JSON dependencies. First configuration needs network
access. Existing dependency checkouts can be supplied with CMake's standard
`FETCHCONTENT_SOURCE_DIR_<NAME>` overrides; record their revisions when doing so.

From the repository root on Linux:

```sh
cmake -S tests/eigen_twist -B build-eigen-twist -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++-21 -DCMAKE_BUILD_TYPE=Debug
cmake --build build-eigen-twist --parallel 4
ctest --test-dir build-eigen-twist --output-on-failure -V
```

On macOS with Homebrew LLVM 21, use
`-DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@21/bin/clang++` and
`-DCMAKE_CXX_FLAGS=-stdlib=libc++`, with
`-DCMAKE_EXE_LINKER_FLAGS="-L/opt/homebrew/opt/llvm@21/lib/c++ -Wl,-rpath,/opt/homebrew/opt/llvm@21/lib/c++"`
to use its matching C++23 library. If the host's SDK discovery is unavailable,
also set `CMAKE_OSX_SYSROOT` to the installed macOS SDK directory.

The queue tests compare accepted and consumed task IDs over 640 seeded schedules.
The allocator tests check cross-thread return and exactly-once destruction over
512 schedules. Scheduler tests check ordinary work and parallel loops against
per-index counts and a serial result, including cancellation and pool shutdown.
The scheduler target also exercises fixed Rapid groups with two, three and nine
workers and checks that a delayed cohort join cannot complete a newer dispatch.

A failed scenario prints its name and seed. These are bounded simulations.
