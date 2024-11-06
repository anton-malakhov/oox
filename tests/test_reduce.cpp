#include <examples/reduce.h>

#include <gtest/gtest.h>
#include <vector>

#define println(s, ...) fprintf(stderr, s "\n",  __VA_ARGS__)

using namespace Reduce;

TEST(Reduce, Works) {
    {
        auto serial = Serial::reduce(0, 100, 0, std::plus<int>{});
        auto oox_recursive = OOX_recursive::reduce(0, 100, 0, std::plus<int>{});
        ASSERT_EQ(serial, oox_recursive.get());
    }

    {
        auto v = std::vector{1, 2, 3};
        auto plus = [](int val, decltype(v)::iterator it) -> int { return val + *it; };

        auto serial = Serial::reduce(v.begin(), v.end(), 0, plus);
        auto oox_recursive = OOX_recursive::reduce(v.begin(), v.end(), 0, plus);
        ASSERT_EQ(serial, oox_recursive.get());
    }
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