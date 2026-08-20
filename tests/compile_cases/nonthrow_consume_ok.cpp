#include <oox/shared_var.h>

struct value {
    value() noexcept = default;
    value(const value&) noexcept = default;
    value(value&&) noexcept = default;
};

void check_nonthrow_consume() {
    oox::shared_var<value, false> deferred(oox::deferred);
    oox::run<false>([](value&) noexcept {}, deferred);

    oox::shared_var<value, false> ready(value{});
    oox::run<false>([](value) noexcept {}, ready);
}
