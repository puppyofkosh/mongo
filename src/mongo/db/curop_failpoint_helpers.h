// TODO: Add message thing at the top

#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/fail_point_service.h"

namespace mongo {

class CurOpFailpointHelpers {
public:
   /**
    * This helper function works much like MONGO_FAIL_POINT_PAUSE_WHILE_SET, but additionally
    * calls whileWaiting() at regular intervals. Finally, it also sets the 'msg' field of the
    * opCtx's CurOp to the given string while the failpoint is active.
    *
    * whileWaiting() may be used to do anything the caller needs done while hanging in the
    * failpoint. For example, the caller may use whileWaiting() to release and reacquire locks in
    * order to avoid deadlocks.
    */
    static void waitWhileFailPointEnabled(FailPoint* failPoint,
                                          OperationContext* opCtx,
                                          const std::string& curOpMsg,
                                          const std::function<void(void)>& whileWaiting = nullptr);

};
    
}
