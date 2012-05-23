package org.vertx.java.busmods.amqp;

import org.vertx.java.framework.TestClientBase;

import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Handler;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import java.util.logging.Logger;
import java.util.logging.Level;

public class AmqpBridgeTestClient extends TestClientBase implements AmqpBridgeTests {
    private static final String AMQP_URI = "amqp://localhost";
    private static final String AMQP_BRIDGE_ADDR = "test.amqpBridge";

    private final Logger logger = Logger.getLogger(getClass().getName());

    private Connection amqpConn;
    private Channel chan;
    private String amqpQueue;

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();

        JsonObject config = new JsonObject();
        config.putString("address", AMQP_BRIDGE_ADDR);
        config.putString("uri", AMQP_URI);

        container.deployVerticle(AmqpBridge.class.getName(), config, 1, new SimpleHandler() {
            public void handle() {
                logger.fine("app is ready");

                tu.appReady();
            }
        });

        try {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setUri(AMQP_URI);

            amqpConn = cf.newConnection();
            chan = amqpConn.createChannel();

            amqpQueue = chan.queueDeclare().getQueue();
        } catch (Exception e) {
            String msg = "unable to set up AMQP connection";
            logger.log(Level.SEVERE, msg, e);
            tu.azzert(false, msg);
        }
    }
    // }}}

    // {{{ stop
    /** {@inheritDoc} */
    @Override
    public void stop() {
        amqpConn.abort();
        amqpConn = null;

        super.stop();
    }
    // }}}

    // {{{ testFoo
    /** {@inheritDoc} */
    public void testFoo() {
        EventBus eb = getVertx().eventBus();

        String handlerId = eb.registerHandler(new Handler<Message<JsonObject>>() {
            // {{{ handle
            /** {@inheritDoc} */
            @Override
            public void handle(final Message<JsonObject> msg) {
                logger.fine("received msg: " + msg.body);

                logger.fine("reply address: " + msg.replyAddress);

                tu.azzert("raw_xbee_frames".equals(msg.body.getString("exchange")), "wrong exchange");

                String contentType = msg.body.getObject("properties").getString("contentType");

                Object body;

                if (
                    "application/json".equals(contentType) ||
                    "application/bson".equals(contentType)
                ) {
                    body = msg.body.getObject("body");
                } else {
                    body = msg.body.getBinary("body");
                }

                logger.fine("received body class: " + body.getClass());
                logger.fine("received body: " + body);
                tu.azzert(body != null, "no body in message");

                tu.testComplete();
            }
            // }}}
        });

        logger.fine("address for registered handler: " + handlerId);

        JsonObject createMsg = new JsonObject();
        createMsg.putString("exchange", "raw_xbee_frames");
        createMsg.putString("routingKey", "*.*");
        createMsg.putString("forward", handlerId);

        eb.send(AMQP_BRIDGE_ADDR + ".create-consumer", createMsg);
    }
    // }}}

    // {{{ testInvokeRpcWithSingleReply
    /**
     * Tests RPC-style message passing.  Sets up an AMQP Consumer -- independent
     * from Vert.x -- that returns a canned response for every invocation
     * received.  This ensures the following paradigm (in JavaScript) works:
     *
     * var req = {
     *     routingKey: "…",
     *     body: {
     *         …
     *     }
     * };
     *
     * eb.send("rabbit.invoke_rpc", req, function(resp) {
     *     // handle response
     * });
     */
    public void testInvokeRpcWithSingleReply() {
        final JsonObject cannedJsonResponse =
            new JsonObject().putString("baz", "bap");

        EventBus eb = getVertx().eventBus();

        // build the endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                logger.fine("in handleDelivery: " + new String(body));

                getChannel().basicAck(envelope.getDeliveryTag(), false);

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                    .correlationId(props.getCorrelationId())
                    .build();

                getChannel().basicPublish(
                    "",
                    props.getReplyTo(),
                    replyProps,
                    cannedJsonResponse.encode().getBytes()
                );
            }
        };

        try {
            chan.basicConsume(amqpQueue, cons);
        } catch (IOException e) {
            String msg = "unable to consume";
            logger.log(Level.SEVERE, msg, e);
            tu.azzert(false, msg);
        }

        // setup is done; fire off the EventBus invocation
        logger.fine("calling .invoke_rpc");

        eb.send(
            AMQP_BRIDGE_ADDR + ".invoke_rpc",
            new JsonObject()
                .putString("routingKey", amqpQueue)
                .putObject("body", new JsonObject().putString("foo", "bar")),
            new Handler<Message<JsonObject>>() {
                // {{{ handle
                /** {@inheritDoc} */
                @Override
                public void handle(final Message<JsonObject> msg) {
                    logger.fine("received msg: " + msg.body);

                    tu.azzert(cannedJsonResponse.equals(msg.body.getObject("body")));

                    tu.testComplete();
                }
                // }}}
            }
        );
    }
    // }}}

    // {{{ testInvokeRpcWithMultipleReplies
    public void testInvokeRpcWithMultipleReplies() {
        EventBus eb = getVertx().eventBus();

        final int replyCount = 3;
        final List<Message<JsonObject>> receivedMessages = new ArrayList<>();

        // set up an anonymous EventBus handler for receiving our RPC responses
        String handlerId = eb.registerHandler(new Handler<Message<JsonObject>>() {
            // {{{ handle
            /** {@inheritDoc} */
            @Override
            public void handle(final Message<JsonObject> msg) {
                receivedMessages.add(msg);

                logger.fine(msg.body.encode());

                if (receivedMessages.size() == replyCount) {
                    tu.testComplete();
                }
            }
            // }}}
        });

        // build the AMQP client endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                logger.fine("in handleDelivery: " + new String(body));

                getChannel().basicAck(envelope.getDeliveryTag(), false);

                java.util.Map<String,Object> headers = new java.util.HashMap<>();
                headers.put("qwerty", "uiop");

                AMQP.BasicProperties replyProps =
                    new AMQP.BasicProperties.Builder()
                        .correlationId(props.getCorrelationId())
                        .headers(headers)
                        .type("Homer")
                        .build();

                for (int i = 1; i <= replyCount; i++) {
                    getChannel().basicPublish(
                        "",
                        props.getReplyTo(),
                        replyProps,
                        new JsonObject().putString(
                            "body",
                            String.format("reply %d of %d", i, replyCount)
                        ).encode().getBytes("UTF-8")
                    );
                }
            }
        };

        try {
            chan.basicConsume(amqpQueue, cons);
        } catch (IOException e) {
            String msg = "unable to consume";
            logger.log(Level.SEVERE, msg, e);
            tu.azzert(false, msg);
        }

        // setup is done; fire off the EventBus invocation
        logger.fine("calling .invoke_rpc");

        eb.send(
            AMQP_BRIDGE_ADDR + ".invoke_rpc",
            new JsonObject()
                .putString("routingKey", amqpQueue)
                .putString("replyTo", handlerId)
                .putObject("body", new JsonObject().putString("foo", "bar"))
        );
    }
    // }}}
}
