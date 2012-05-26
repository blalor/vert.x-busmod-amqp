package org.vertx.java.busmods.amqp;

import org.vertx.java.framework.TestBase;
import org.vertx.java.framework.TestUtils;

import org.vertx.java.core.json.JsonObject;

import java.util.logging.Logger;
import java.util.logging.Level;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

import java.io.IOException;

public class AmqpBridgeJavaScriptIT extends TestBase {
    private final Logger logger = Logger.getLogger(getClass().getName());

    private static final String AMQP_URI = "amqp://localhost";

    private Connection amqpConn;
    private Channel chan;
    private String amqpQueue;
    private TestUtils tu;

    // {{{ setUp
    /** {@inheritDoc} */
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        ConnectionFactory cf = new ConnectionFactory();
        cf.setUri(AMQP_URI);

        amqpConn = cf.newConnection();
        chan = amqpConn.createChannel();

        amqpQueue = chan.queueDeclare().getQueue();

        tu = new TestUtils(vertx);

        logger.finer("starting js test client");

        startApp(
            "amqp_test_client.js",
            new JsonObject()
                .putString("address", "amqp_test")
                .putString("uri", AMQP_URI)
                .putString("defaultContentType", "application/json")

                // data for the test itself
                .putObject(
                    "test_data",
                    new JsonObject()
                        .putString("tgt:routingKey", amqpQueue)
                )
        );
    }
    // }}}

    // {{{ tearDown
    /** {@inheritDoc} */
    @Override
    public void tearDown() throws Exception {
        amqpConn.abort();

        amqpConn = null;
        chan = null;
        amqpQueue = null;

        super.tearDown();
    }
    // }}}

    // {{{ testSendJson
    public void testSendJson() throws Exception {
        // build the endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                getChannel().basicAck(envelope.getDeliveryTag(), false);

                JsonObject receivedBody = new JsonObject(new String(body));

                logger.fine("in handleDelivery: " + receivedBody);

                tu.azzert("bar".equals(receivedBody.getString("foo")));

                tu.testComplete();

                // AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                //     .correlationId(props.getCorrelationId())
                //     .build();

                // getChannel().basicPublish(
                //     "",
                //     props.getReplyTo(),
                //     replyProps,
                //     cannedJsonResponse.encode().getBytes()
                // );
            }
        };

        // channel.queueBind(amqpQueue, "", getContainer().getConfig().getObject("test_data"));
        chan.basicConsume(amqpQueue, cons);

        startTest(getMethodName());
    }
    // }}}

    // {{{ testSendPlainText
    public void testSendPlainText() throws Exception {
        // build the endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                getChannel().basicAck(envelope.getDeliveryTag(), false);

                String receivedBody = new String(body);

                logger.fine("in handleDelivery: " + receivedBody);

                tu.azzert("foo bar".equals(receivedBody));

                tu.testComplete();

            }
        };

        chan.basicConsume(amqpQueue, cons);

        startTest(getMethodName());
    }
    // }}}

    // {{{ testInvokeRpcWithSingleReply
    public void testInvokeRpcWithSingleReply() throws Exception {
        // build the endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                getChannel().basicAck(envelope.getDeliveryTag(), false);

                JsonObject receivedMsg = new JsonObject(new String(body));
                
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                    .correlationId(props.getCorrelationId())
                    .contentType("text/plain")
                    .build();

                getChannel().basicPublish(
                    "",
                    props.getReplyTo(),
                    replyProps,
                    String.format("Hi, %s!", receivedMsg.getString("my_name")).getBytes()
                );

            }
        };

        chan.basicConsume(amqpQueue, cons);

        startTest(getMethodName());
    }
    // }}}

    // {{{ testInvokeRpcWithManyReplies
    public void testInvokeRpcWithManyReplies() throws Exception {
        // build the endpoint for the module to deliver to
        Consumer cons = new DefaultConsumer(chan) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties props,
                                       final byte[] body)
                throws IOException
            {
                getChannel().basicAck(envelope.getDeliveryTag(), false);

                JsonObject receivedMsg = new JsonObject(new String(body));

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                    .correlationId(props.getCorrelationId())
                    .contentType("text/plain")
                    .build();

                for (int i = 1; i <= receivedMsg.getNumber("reply_count").intValue(); i++) {
                    getChannel().basicPublish(
                        "",
                        props.getReplyTo(),
                        replyProps,
                        String.format("reply %d of %d", i, receivedMsg.getNumber("reply_count")).getBytes()
                    );
                }
            }
        };

        chan.basicConsume(amqpQueue, cons);

        startTest(getMethodName());
    }
    // }}}

    // {{{ testCreateConsumer
    public void testCreateConsumer() throws Exception {
        startTest(getMethodName());
    }
    // }}}
}
