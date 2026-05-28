#include <oox/oox.h>

#include "oox_twist_harness.h"

namespace {

void ReturnDeferredVarFromTask() {
    [[maybe_unused]] auto forwarded = oox::run([]() -> oox::var<int> {
        return oox::var<int>(oox::deferred);
    });
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("ReturnDeferredVarFromTask", ReturnDeferredVarFromTask);
    return 0;
}
