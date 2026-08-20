#include <oox/shared_var.h>

struct converted {
    converted(int) { throw 1; }
};

void check_cross_type_conversion_is_rejected() {
    oox::shared_var<int, false> ready(1);
    oox::run<false>([](converted) noexcept {}, ready);
}
