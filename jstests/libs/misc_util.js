"use strict";

/**
 * Random util functions neeeded in tests.
 */
var MiscUtil = (function() {

    /**
     * Check if aSet and bSet are equal.
     */
    function setEq(aSet, bSet) {
        if (aSet.size != bSet.size) {
            return false;
        }
        for (var a of aSet) {
            if (!bSet.has(a)) {
                return false;
            }
        }
        return true;
    }

    return {
        setEq: setEq,
    };
})();
