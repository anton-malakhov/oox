#include <oox/shared_var.h>

struct value {
    value() noexcept = default;
    value(const value&) noexcept = default;
    value(value&&) noexcept = default;
};

struct safe_copy_value {
    safe_copy_value() noexcept = default;
    safe_copy_value(const safe_copy_value&) noexcept = default;
    safe_copy_value(safe_copy_value&&) { throw 1; }
};

void check_nonthrow_consume() {
    oox::shared_var<value, false> deferred(oox::deferred);
    oox::run<false>([](value&) noexcept {}, deferred);

    oox::shared_var<value, false> ready(value{});
    oox::run<false>([](value) noexcept {}, ready);

    oox::shared_var<safe_copy_value, false> copied;
    oox::run<false>([](safe_copy_value&) noexcept {}, copied);

    oox::run<false>([](int = 42) noexcept {});
    oox::run<false>([](int, int = 2) noexcept {}, 40);
}
