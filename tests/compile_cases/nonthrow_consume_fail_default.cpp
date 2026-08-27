#include <oox/shared_var.h>

struct value {
    value() { throw 1; }
    value(const value&) noexcept = default;
    value(value&&) noexcept = default;
};

void check_throwing_default_is_rejected() {
    oox::shared_var<value, false> deferred(oox::deferred);
    oox::run<false>([](value&) noexcept {}, deferred);
}
