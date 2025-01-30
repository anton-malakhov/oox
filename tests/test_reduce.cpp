#include <examples/reduce.h>

#include <gtest/gtest.h>
#include <vector>

#define println(s, ...) fprintf(stderr, s "\n",  __VA_ARGS__)

using namespace Reduce;

TEST(Reduce, Ints) {
    auto serial = Serial::reduce(0, 128, 0, std::plus<int>{});
    auto oox_serial = OOX_serial::reduce(0, 128, 0, std::plus<int>{});
    auto oox_parallel = OOX_parallel::reduce(0, 128, 0, std::plus<int>{}, std::plus<int>{}); 
    auto oox_parallel_n = OOX_parallel_n::reduce<2>(0, 128, 0, std::plus<int>{}, std::plus<int>{});
    ASSERT_EQ(serial, oox_serial.get());
    ASSERT_EQ(serial, oox_parallel.get());
    ASSERT_EQ(serial, oox_parallel_n.get());
}

TEST(Reduce, Iterators) {
    auto v = std::vector<int>();
    for (int i = 0; i < 128; ++i) { v.push_back(i); }
    auto plus = [](int val, decltype(v)::iterator it) -> int { return val + *it; };

    auto serial = Serial::reduce(v.begin(), v.end(), 0, plus);
    auto oox_serial = OOX_serial::reduce(v.begin(), v.end(), 0, plus);
    auto oox_parallel = OOX_parallel::reduce(v.begin(), v.end(), 0, plus, std::plus<int>{});
    auto oox_parallel_n = OOX_parallel_n::reduce<2>(v.begin(), v.end(), 0, plus, std::plus<int>{});
    ASSERT_EQ(serial, oox_serial.get());
    ASSERT_EQ(serial, oox_parallel.get());
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    int err{0};
    try {
        err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
        println("Error: %s", e.what());
    }
    return err;
}
