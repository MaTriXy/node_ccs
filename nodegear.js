var config = require('./gcmConfig');
var async = require("async");
var uuid = require('node-uuid');
var gearmanode = require('gearmanode');
var redis = require("redis");
var xmpp = require('node-xmpp');
var XmppClient = require('node-xmpp-client');

var connectionId;
var gearJob;
var redisSubChan;
var gearmanJobName = 'lollyLollypopGearBundleServicesMessageConsumer~gcm';
var jobPayload;
var redisClient, redisClient2;
var xmppClient;
var gearClient;

var lastActive = new Date();
var cleanedUp = false;

var listOfProcess = "xmpp_node_processes";
var lastActv = "_la";
var maxIdleTime = 3600000;

redisSubChan = uuid.v4();

gearClient = gearmanode.client({loadBalancing: 'RoundRobin'});

var options = {
    type: 'client',
    jid: config.jid,
    password: config.password,
    port: config.port,
    host: config.host,
    legacySSL: true,
    preferredSaslMechanism: 'PLAIN'
};

xmppClient = new XmppClient(options);

xmppClient.connection.socket.setTimeout(0);
xmppClient.connection.socket.setKeepAlive(true, 10000);

redisClient = redis.createClient();
redisClient2 = redis.createClient();

redisClient2.lpush(listOfProcess, redisSubChan);
redisClient2.set(redisSubChan + lastActv, lastActive.toString());

redisClient.subscribe(redisSubChan);

redisClient.on("message", function (channel, message) {
    switch (channel) {
        case redisSubChan:
            var toDevice = new xmpp.Element('message', {'id': ''}).c('gcm', {xmlns: 'google:mobile:data'}).t(message);
            xmppClient.send(toDevice);
    }
});

xmppClient.on('online', function () {
    console.log("online");
});

xmppClient.on('connection', function () {
    console.log('connection');
});

xmppClient.on('connect', function () {
    console.log('Client is connected');
});

xmppClient.on('stanza',
        function (stanza) {
            lastActive = new Date();
            redisClient2.set(redisSubChan + lastActv, lastActive.toString());
            if (stanza.is('message') && stanza.attrs.type !== 'error') {
                // Best to ignore an error
                //Message format as per here: https://developer.android.com/google/gcm/ccs.html#upstream
                var messageData = JSON.parse(stanza.getChildText("gcm"));
                messageData.channel = redisSubChan;
                var messageType = messageData.message_type;

                if (messageType != "ack" && messageType != "nack" && messageType != "receipt" && messageType != "control") {

                    var ackMsg = new xmpp.Element('message', {'id': ''}).c('gcm', {xmlns: 'google:mobile:data'}).t(JSON.stringify({
                        "to": messageData.from,
                        "message_id": messageData.message_id,
                        "message_type": "ack"
                    }));
                    //send back the ack.
                    xmppClient.send(ackMsg);
                }

                //receive messages from ccs and give it to PHP workers
                var jobName;
                if (typeof messageType === "undefined") {
                    jobName = gearmanJobName + '_' + 'msg';
                } else {
                    jobName = gearmanJobName + '_' + messageType;
                }
                gearClient.submitJob(jobName, JSON.stringify(messageData), {background: true, priority: 'HIGH', encoding: 'utf8', unique: messageData.message_id});
            } else {
                console.log("stanza error");
            }
            console.log(stanza.toString());
        });

xmppClient.on('authenticate', function (opts, cb) {
    console.log('AUTH' + opts.jid + ' -> ' + opts.password);
    cb(null);
});

xmppClient.on('close', function () {
    console.log('close');
    if (!cleanedUp) {
        cleanUp();
    }
});

xmppClient.on('disconnect', function () {
    console.log('disconnect');
    if (!cleanedUp) {
        cleanUp();
    }
});

xmppClient.on('end', function () {
    console.log('end');
    if (!cleanedUp) {
        cleanUp();
    }
});

xmppClient.on('error', function (e) {
    console.log("Error occured:");
    console.error(e);
    console.error(e.children);
    if (!cleanedUp) {
        cleanUp();
    }
});

xmppClient.on('offline', function (e) {
    console.log("offline");
    if (!cleanedUp) {
        cleanUp();
    }
});

process.on('exit', function () {
    console.log("exit");
});

function cleanUp() {
    redisClient2.lrem(listOfProcess, 0, redisSubChan, function (err) {

    });
    redisClient2.del(redisSubChan + lastActv);
    cleanedUp = true;
    redisClient.quit();
    redisClient2.quit();
    gearClient.close();
    xmppClient.end();
    process.exit(0);
}

function isItTimeToDie() {
    //find the average of all processes, if idle time is more than this
    var overallIdleAvgTime = 0;
    var numOfProcesses = 0;
    var idleTime = 0;

    redisClient2.lrange(listOfProcess, 0, -1, function (err, processIds) {
        async.each(processIds,
                function (processId, callback) {
                    redisClient2.get(processId + lastActv, function (err, lastATime) {
                        if (!err) {
                            numOfProcesses++;
                            var currentDate = new Date();
                            var lastAcTime = new Date(lastATime);
                            if (lastAcTime.getTime() > maxIdleTime) {
                                maxIdleTime = lastAcTime.getTime();
                            }
                            idleTime += Math.abs(currentDate.getTime() - lastAcTime.getTime());
                        } else {
                            redisClient2.lrem(listOfProcess, 0, processId + lastActv, function (err) {

                            });
                        }
                        callback(null);
                    });
                },
                function (err) {
                    if (numOfProcesses > 1) {
                        overallIdleAvgTime = idleTime / numOfProcesses;

                        var currentDate = new Date();
                        var timeDiff = Math.abs(currentDate.getTime() - lastActive.getTime());

                        if (overallIdleAvgTime > 0 && timeDiff > overallIdleAvgTime) {
                            cleanUp();
                        }
                    }
                }
        );
    });

}

setInterval(isItTimeToDie, 3600000);//every hour
