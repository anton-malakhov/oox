#include <oox/shared_var.h>

struct value {
    value() noexcept = default;
    value(const value&) { throw 1; }
    value(value&&) noexcept = default;
};

void check_actual_copy_category_is_rejected() {
    oox::shared_var<value, false> ready(value{});
    oox::run<false>([](value) noexcept {}, ready);
}
