load('test_utils.js');
load('vertx.js');

var tu = new TestUtils();

var eb = vertx.eventBus;
var _config = vertx.config;
var logger = vertx.logger;

function testSendJson() {
    eb.send(
        _config.address + ".send",
        {
            "routingKey" : _config.test_data["tgt:routingKey"],
            "body" : {"foo":"bar"}
        },
        function(reply) {
            tu.azzert(reply.status == "ok");
        }
    );
}

function testSendPlainText() {
    eb.send(
        _config.address + ".send",
        {
            "routingKey" : _config.test_data["tgt:routingKey"],
            "properties" : {
                "contentType" : "text/plain"
            },
            "body" : "foo bar"
        },
        function(reply) {
            tu.azzert(reply.status == "ok");
        }
    );
}

function testInvokeRpcWithSingleReply() {
    eb.send(
        _config.address + ".invoke_rpc",
        {
            "routingKey" : _config.test_data["tgt:routingKey"],
            "body" : {
                "my_name" : "Bob"
            }
        },
        function(reply) {
            logger.debug(JSON.stringify(reply));

            tu.azzert(reply.body == "Hi, Bob!");
            tu.testComplete();
        }
    );
}

function testInvokeRpcWithManyReplies() {
    var handlerAddr = "testInvokeRpcWithManyReplies";

    eb.registerHandler(handlerAddr, function(msg) {
        if (msg.body == "reply 3 of 3") {
            tu.testComplete();
        }
    });

    eb.send(
        _config.address + ".invoke_rpc",
        {
            "routingKey" : _config.test_data["tgt:routingKey"],
            "replyTo" : handlerAddr,
            "body" : {
                "reply_count" : 3
            }
        },
        function(reply) {
            tu.azzert(reply.status == "ok");
        }
    );
}

vertx.deployWorkerVerticle(
    "org.vertx.java.busmods.amqp.AmqpBridge",
    _config,
    1,
    function() {
        logger.debug("registering tests");
        tu.registerTests(this);

        logger.debug("sending appReady");
        tu.appReady();
    }
);

function vertxStop() {
    tu.unregisterAll();
    tu.appStopped();
}
