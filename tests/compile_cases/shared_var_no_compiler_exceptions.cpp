#include "oox/shared_var.h"

int probe_shared_var_without_compiler_exceptions() {
    oox::shared_var<int, false> value;
    auto written = oox::run<false>([](int& output) noexcept {
        output = 42;
    }, value);
    oox::wait_for_all(written);
    return value.get();
}
