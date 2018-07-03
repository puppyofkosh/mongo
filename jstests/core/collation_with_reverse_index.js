// Regression test for SERVER-34846.
(function() {
    db.stringtest.insertOne({int: 1, text: "hello world"});
    db.stringtest.createIndex({int: -1, text: -1}, {collation: {locale: "en", strength: 1}});
    const res = db.stringtest.find({int: 1}, {_id: 0, int: 1, text: 1}).toArray();

    assert.eq(res[0].text, "hello world");
})();
