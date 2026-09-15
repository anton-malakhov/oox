#include <oox/shared_var.h>

static_assert(__cplusplus >= 202002L, "OOX::OOX must propagate its C++20 requirement");

int main() {
    oox::shared_var<int> value(42);
    return value.get() == 42 ? 0 : 1;
}
