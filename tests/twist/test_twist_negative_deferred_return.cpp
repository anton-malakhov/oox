#include <oox/oox.h>

#include "oox_twist_harness.h"

namespace {

void ReturnDeferredVarFromTask() {
    auto forwarded = oox::run([]() -> oox::var<int> {
        return oox::var<int>(oox::deferred);
    });

    (void)forwarded;
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("ReturnDeferredVarFromTask", ReturnDeferredVarFromTask);
    return 0;
}
