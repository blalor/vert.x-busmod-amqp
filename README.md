AMQP bridge busmod for Vert.x
=============================

This busmod is a bridge between Vert.x and an AMQP message broker (such as [RabbitMQ][rabbit]).  This code was originally developed on a branch of the main [Vert.x][vertx] project, but I extended it, added new features, and some tests.

Usage
-----

### load the verticle

Programmatically, if you like:

    vertx.deployWorkerVerticle(
        "org.vertx.java.busmods.amqp.AmqpBridge",
        {
            "uri": "amqp://localhost",
            "address": "amqp_bridge",
            "defaultContentType": "application/json"
        },
        1,
        function() {
            // subscribe, if necessary, or send initial message
        }
    );


### subscribe to a queue or topic

    var handlerAddr = "my_addr";

    eb.registerHandler(handlerAddr, function(msg) {
        // handle message!
    });

    eb.send(
        "amqp_bridge.create-consumer",
        {
            "exchange" : "raw_xbee_frames",
            "routingKey" : "#",
            "forward" : handlerAddr
        },
        function(reply) {
            // check reply.status == "ok"
        }
    );

### send a message to a queue or topic

    eb.send(
        ""amqp_bridge.send",
        {
            "routingKey" : "target_queue"
            "properties" : {
                "contentType" : "text/plain"
            },
            "body" : "my message"
        },
        function(reply) {
            // check reply.status == "ok"
        }
    );

### invoke an RPC method

Based on [this RabbitMQ article][rabbit_tut].

    eb.send(
        "amqp_bridge.invoke_rpc",
        {
            "routingKey" : "target_queue",
            "body" : {
                "my_name" : "Bob"
            }
        },
        function(reply) {
            // check reply.body == "Hi, Bob!"
        }
    );

### invoke an RPC method with multiple responses

    var handlerAddr = "my_handler";

    eb.registerHandler(handlerAddr, function(msg) {
        // do something with msg
    });

    eb.send(
        ""amqp_bridge.invoke_rpc",
        {
            "routingKey" : "target_queue",
            "replyTo" : handlerAddr,
            "body" : {
                "times_to_reply" : 3
            }
        },
        function(reply) {
            // check reply.status == "ok"
        }
    );


[rabbit]: http://www.rabbitmq.com/
[vertx]: http://vertx.io/
[rabbit_tut]: http://www.rabbitmq.com/tutorials/tutorial-six-java.html
