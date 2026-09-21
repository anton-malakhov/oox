#include <oox/shared_var.h>

struct value {
    value() = default;
    value(const value&) = delete;
    value(value&&) = delete;
};

void check_immovable_value_is_rejected() {
    oox::shared_var<value, false> invalid;
}
