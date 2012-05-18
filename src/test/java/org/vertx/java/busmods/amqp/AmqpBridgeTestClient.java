package org.vertx.java.busmods.amqp;

import org.vertx.java.framework.TestClientBase;

import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Handler;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;

import java.util.Map;

import java.util.logging.Logger;

public class AmqpBridgeTestClient extends TestClientBase implements AmqpBridgeTests {
    private static final String AMQP_BRIDGE_ADDR = "test.amqpBridge";

    private final Logger logger = Logger.getLogger(getClass().getName());

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();

        JsonObject config = new JsonObject();
        config.putString("address", AMQP_BRIDGE_ADDR);
        config.putString("uri", "amqp://pepe");

        container.deployVerticle(AmqpBridge.class.getName(), config, 1, new SimpleHandler() {
            public void handle() {
                logger.fine("app is ready");

                tu.appReady();
            }
        });
    }
    // }}}

    // {{{ testFoo
    /** {@inheritDoc} */
    public void testFoo() {
        logger.fine("in testFoo");

        EventBus eb = getVertx().eventBus();

        String handlerId = eb.registerHandler(new Handler<Message<JsonObject>>() {
            // {{{ handle
            /** {@inheritDoc} */
            @Override
            public void handle(final Message<JsonObject> msg) {
                logger.fine("received msg: " + msg.body);

                JsonObject envelope = msg.body.getObject("envelope");
                tu.azzert(envelope != null, "no envelope in message");
                tu.azzert("sensor_data".equals(envelope.getString("exchange")), "wrong exchange");

                JsonObject body = msg.body.getObject("body");
                tu.azzert(body != null, "no body in message");

                tu.testComplete();
            }
            // }}}
        });

        logger.fine("address for registered handler: " + handlerId);

        JsonObject createMsg = new JsonObject();
        createMsg.putString("exchange", "sensor_data");
        createMsg.putString("routing_key", "*");
        createMsg.putString("forward", handlerId);

        eb.send(AMQP_BRIDGE_ADDR + ".create-consumer", createMsg);

    }
    // }}}
}
