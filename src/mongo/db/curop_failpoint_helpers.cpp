// TODO: Add a giant header thing

#include "mongo/platform/basic.h"

#include "mongo/db/curop_failpoint_helpers.h"

#include "mongo/db/curop.h"


namespace mongo {

namespace {

// Helper function which sets the 'msg' field of the opCtx's CurOp to the specified string, and
// returns the original value of the field.
std::string updateCurOpMsg(OperationContext* opCtx, const std::string& newMsg) {
    stdx::lock_guard<Client> lk(*opCtx->getClient());
    auto oldMsg = CurOp::get(opCtx)->getMessage();
    CurOp::get(opCtx)->setMessage_inlock(newMsg.c_str());
    return oldMsg;
}

}  // namespace
    
void CurOpFailpointHelpers::waitWhileFailPointEnabled(FailPoint* failPoint,
                                                      OperationContext* opCtx,
                                                      const std::string& curOpMsg,
                                                      const std::function<void(void)>& whileWaiting) {
    invariant(failPoint);
    auto origCurOpMsg = updateCurOpMsg(opCtx, curOpMsg);

    MONGO_FAIL_POINT_BLOCK((*failPoint), options) {
        const BSONObj& data = options.getData();
        const bool shouldCheckForInterrupt = data["shouldCheckForInterrupt"].booleanSafe();
        while (MONGO_FAIL_POINT((*failPoint))) {
            sleepFor(Milliseconds(10));
            if (whileWaiting) {
                whileWaiting();
            }

            // Check for interrupt so that an operation can be killed while waiting for the
            // failpoint to be disabled, if the failpoint is configured to be interruptible.
            if (shouldCheckForInterrupt) {
                opCtx->checkForInterrupt();
            }
        }
    }

    updateCurOpMsg(opCtx, origCurOpMsg);
}
}
