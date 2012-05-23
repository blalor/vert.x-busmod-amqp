package org.vertx.java.busmods.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;

import java.net.URISyntaxException;

import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;

import java.util.UUID;

/**
 * Prototype for AMQP bridge
 * Currently only does pub/sub and does not declare exchanges so only works with default exchanges
 * Three operations:
 * 1) Create a consumer on a topic given exchange name (use amqp.topic) and routing key (topic name)
 * 2) Close a consumer
 * 3) Send message
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 */
public class AmqpBridge extends BusModBase {
    private Connection conn;
    private Map<Long, Channel> consumerChannels = new HashMap<>();
    private long consumerSeq;
    private Queue<Channel> availableChannels = new LinkedList<>();

    private String callbackQueue;
    private RPCCallbackHandler rpcCallbackHandler;

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();

        String address = getMandatoryStringConfig("address");
        String uri = getMandatoryStringConfig("uri");

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        } catch (KeyManagementException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        }

        try {
            conn = factory.newConnection(); // IOException
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create connection", e);
        }

        try {
            rpcCallbackHandler = new RPCCallbackHandler(getChannel(), eb);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create queue for callbacks", e);
        }

        // register handlers
        eb.registerHandler(address + ".create-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleCreateConsumer(message);
            }
        });

        eb.registerHandler(address + ".close-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleCloseConsumer(message);
            }
        });

        eb.registerHandler(address + ".send", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleSend(message);
            }
        });

        eb.registerHandler(address + ".invoke_rpc", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleInvokeRPC(message);
            }
        });
    }
    // }}}

    // {{{ stop
    /** {@inheritDoc} */
    @Override
    public void stop() {
        consumerChannels.clear();

        try {
            conn.close();
        } catch (Exception e) {
            logger.error("Failed to close", e);
        }
    }
    // }}}

    // {{{ getChannel
    private Channel getChannel() throws IOException {
        if (! availableChannels.isEmpty()) {
            return availableChannels.remove();
        } else {
            return conn.createChannel(); // IOException
        }
    }
    // }}}

    // {{{ send
    private void send(final AMQP.BasicProperties props, final JsonObject message)
        throws IOException
    {
        AMQP.BasicProperties.Builder amqpPropsBuilder = new AMQP.BasicProperties.Builder();

        if (props != null) {
            amqpPropsBuilder = props.builder();
        }

        // correlationId and replyTo will already be set, if necessary
        amqpPropsBuilder.contentEncoding("UTF-8");

        JsonObject ebProps = message.getObject("properties");
        if (ebProps != null) {
            amqpPropsBuilder.clusterId(ebProps.getString("clusterId"));
            amqpPropsBuilder.contentType(ebProps.getString("contentType"));
            amqpPropsBuilder.deliveryMode(ebProps.getNumber("deliveryMode").intValue());
            amqpPropsBuilder.expiration(ebProps.getString("expiration"));

            if (ebProps.getObject("headers") != null) {
                logger.debug("headers: " + ebProps.getObject("headers"));
                
                amqpPropsBuilder.headers(ebProps.getObject("headers").toMap());
            }

            amqpPropsBuilder.messageId(ebProps.getString("messageId"));
            amqpPropsBuilder.priority(ebProps.getNumber("priority").intValue());

            // amqpPropsBuilder.timestamp(ebProps.getString("timestamp")); // @todo
            amqpPropsBuilder.type(ebProps.getString("type"));
            amqpPropsBuilder.userId(ebProps.getString("userId"));
        }
        
        Channel channel = getChannel();

        availableChannels.add(channel);
        try {
            channel.basicPublish(
                // exchange must default to non-null string
                message.getString("exchange", ""),
                message.getString("routingKey"),
                amqpPropsBuilder.build(),
                // @todo transform json -> bytes based on content type
                message.getObject("body").encode().getBytes("UTF-8")
            );
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is not supported, eh?  Really?", e);
        }
    }
    // }}}

    // {{{ createConsumer
    private long createConsumer(final String exchangeName,
                                final String routingKey,
                                final String forwardAddress)
        throws IOException
    {
        Channel channel = getChannel();

        Consumer cons = new MessageTransformingConsumer(channel) {
            public void doHandle(final String consumerTag,
                                 final Envelope envelope,
                                 final AMQP.BasicProperties properties,
                                 final JsonObject body)
                throws IOException
            {
                long deliveryTag = envelope.getDeliveryTag();

                eb.send(forwardAddress, body);

                getChannel().basicAck(deliveryTag, false);
            }
        };

        String queueName = channel.queueDeclare().getQueue();

        channel.queueBind(queueName, exchangeName, routingKey);
        channel.basicConsume(queueName, cons);

        long id = consumerSeq++;
        consumerChannels.put(id, channel);

        return id;
    }
    // }}}

    // {{{ closeConsumer
    private void closeConsumer(final long id) {
        Channel channel = consumerChannels.remove(id);

        if (channel != null) {
            availableChannels.add(channel);
        }
    }
    // }}}

    // {{{ handleCreateConsumer
    private void handleCreateConsumer(final Message<JsonObject> message) {
        logger.debug("Creating consumer: " + message.body);

        String exchange = message.body.getString("exchange");
        String routingKey = message.body.getString("routingKey");
        String forwardAddress = message.body.getString("forward");

        JsonObject reply = new JsonObject();

        try {
            reply.putNumber("id", createConsumer(exchange, routingKey, forwardAddress));

            sendOK(message, reply);
        } catch (IOException e) {
            sendError(message, "unable to create consumer: " + e.getMessage(), e);
        }
    }
    // }}}

    // {{{ handleCloseConsumer
    private void handleCloseConsumer(final Message<JsonObject> message) {
        long id = (Long) message.body.getNumber("id");

        closeConsumer(id);
    }
    // }}}

    // {{{ handleSend
    private void handleSend(final Message<JsonObject> message) {
        try {
            send(null, message.body);

            sendOK(message);
        } catch (IOException e) {
            sendError(message, "unable to send: " + e.getMessage(), e);
        }
    }
    // }}}

    // {{{ handleInvokeRPC
    private void handleInvokeRPC(final Message<JsonObject> message) {
        // if replyTo is non-null, then this is a multiple-response RPC invocation
        String replyTo = message.body.getString("replyTo");

        boolean isMultiResponse = (replyTo != null);

        // the correlationId is what ties this all together.
        String correlationId = UUID.randomUUID().toString();

        AMQP.BasicProperties.Builder amqpPropsBuilder = new AMQP.BasicProperties.Builder()
            .correlationId(correlationId)
            .replyTo(rpcCallbackHandler.getQueueName());

        if (isMultiResponse) {
            // multiple-response invocation
            rpcCallbackHandler.addMultiResponseCorrelation(correlationId, replyTo);
        } else {
            // standard call/response invocation; message.reply() will be called
            rpcCallbackHandler.addCorrelation(correlationId, message);
        }

        try {
            send(amqpPropsBuilder.build(), message.body);

            // always invoke message.reply to avoid ambiguity
            if (isMultiResponse) {
                sendOK(message);
            }
        } catch (IOException e) {
            sendError(message, "unable to publish: " + e.getMessage(), e);
        }
    }
    // }}}
}
