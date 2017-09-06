(function() {
    if (TestData.threadID == 2) {
        throw new Error("made up error");
    }

    sleep(1000);

    return 0;
})();
