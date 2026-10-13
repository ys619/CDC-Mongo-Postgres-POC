/**
 * MongoDB Replica Set Initialization Script for rs0
 * Connects to the isolated mongo-source instance and initiates replica set rs0.
 */
print("==> Checking MongoDB Replica Set Status...");

var isInitiated = false;
try {
    var status = rs.status();
    if (status && status.ok === 1) {
        print("==> Replica set rs0 is already initiated and active.");
        isInitiated = true;
    }
} catch (e) {
    print("==> Replica set not yet initiated: " + e.message);
}

if (!isInitiated) {
    print("==> Initiating replica set rs0 with member mongo-source:27017...");
    var res = rs.initiate({
        _id: "rs0",
        members: [
            { _id: 0, host: "mongo-source:27017", priority: 1 }
        ]
    });
    print("==> rs.initiate() result: " + JSON.stringify(res));
}

print("==> Waiting for primary election...");
var attempts = 30;
while (attempts > 0) {
    try {
        var hello = db.hello ? db.hello() : db.isMaster();
        if (hello.isWritablePrimary || hello.ismaster) {
            print("==> SUCCESS: mongo-source is now PRIMARY (writable). Setup complete.");
            break;
        }
    } catch (e) {
        print("==> Waiting for primary... (" + e.message + ")");
    }
    sleep(1000);
    attempts--;
}

if (attempts === 0) {
    print("==> WARNING: Timed out waiting for PRIMARY election.");
}
