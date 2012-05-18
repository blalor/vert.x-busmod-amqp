package org.vertx.java.busmods.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;

import java.net.URISyntaxException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;

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

    private ConnectionFactory factory;
    private Connection conn;
    private Map<Long, Channel> consumerChannels = new HashMap<>();
    private long consumerSeq;
    private Queue<Channel> availableChannels = new LinkedList<>();

    private DatatypeFactory datatypeFactory;

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();

        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException("unable to get datatype factory", e);
        }

        String address = getMandatoryStringConfig("address");
        String uri = getMandatoryStringConfig("uri");

        factory = new ConnectionFactory();

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
            container.getLogger().error("Failed to create connection", e);
        }

        eb.registerHandler(address + ".create-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                String exchange = message.body.getString("exchange");
                String routingKey = message.body.getString("routing_key");
                String forwardAddress = message.body.getString("forward");

                JsonObject reply = new JsonObject();

                try {
                    reply.putNumber("id", createConsumer(exchange, routingKey, forwardAddress));
                    reply.putString("status", "ok");
                } catch (IOException e) {
                    reply.putString("status", "error");
                    reply.putString("message", "unable to create consumer: " + e.getMessage());
                }

                message.reply(reply);
            }
        });

        eb.registerHandler(address + ".close-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                long id = (Long) message.body.getNumber("id");

                closeConsumer(id);
            }
        });

        eb.registerHandler(address + ".send", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                String exchange = message.body.getString("exchange");
                String routingKey = message.body.getString("routing_key");
                String body = message.body.getString("body");

                JsonObject reply = new JsonObject();

                try {
                    send(exchange, routingKey, body.getBytes("UTF-8"));

                    reply.putString("status", "ok");
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalStateException("UTF-8 is not supported, eh?  Really?", e);
                } catch (IOException e) {
                    container.getLogger().error("Failed to send", e);

                    reply.putString("status", "error");
                    reply.putString("message", "unable to send: " + e.getMessage());
                }

                message.reply(reply);
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
            container.getLogger().error("Failed to close", e);
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
    private void send(final String exchangeName,
                      final String routingKey,
                      final byte[] message)
        throws IOException
    {
        Channel channel = getChannel();

        availableChannels.add(channel); // why?

        channel.basicPublish(exchangeName, routingKey, null, message);
    }
    // }}}

    // {{{ createConsumer
    private long createConsumer(final String exchangeName,
                                final String routingKey,
                                final String forwardAddress)
        throws IOException
    {
        // URRG! AMQP is so clunky :(
        // all this code just to set up a pub/sub consumer

        final Channel channel = getChannel();

        String queueName = channel.queueDeclare().getQueue(); // IOException
        channel.queueBind(queueName, exchangeName, routingKey); // IOException

        Consumer cons = new DefaultConsumer(channel) {
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties properties,
                                       final byte[] body)
                throws IOException
            {
                long deliveryTag = envelope.getDeliveryTag();

                System.out.println("properties: " + properties);

                /*
                    {
                        deliveryTag: Number,
                        exchange: String,
                        routingKey: String,
                        properties: { … },
                        body: { … }
                    }
                */

                // blindly assumes that content is a JSON string; should check content-type…
                JsonObject msg = new JsonObject()
                    // .putNumber("deliveryTag", deliveryTag)
                    .putString("exchange", envelope.getExchange())
                    .putString("routingKey", envelope.getRoutingKey())
                    .putObject("body", new JsonObject(new String(body)));

                if (properties != null) {
                    JsonObject jsonProps = new JsonObject();

                    maybeSetProperty(jsonProps, "appId",           properties.getAppId());

                    // I think these will always be "basic"
                    // maybeSetProperty(jsonProps, "classId",         properties.getClassId());
                    // maybeSetProperty(jsonProps, "className",       properties.getClassName());

                    maybeSetProperty(jsonProps, "clusterId",       properties.getClusterId());
                    maybeSetProperty(jsonProps, "contentEncoding", properties.getContentEncoding());
                    maybeSetProperty(jsonProps, "contentType",     properties.getContentType());
                    maybeSetProperty(jsonProps, "correlationId",   properties.getCorrelationId());
                    maybeSetProperty(jsonProps, "deliveryMode",    properties.getDeliveryMode());
                    maybeSetProperty(jsonProps, "expiration",      properties.getExpiration());
                    maybeSetProperty(jsonProps, "headers",         properties.getHeaders());
                    maybeSetProperty(jsonProps, "messageId",       properties.getMessageId());
                    maybeSetProperty(jsonProps, "priority",        properties.getPriority());
                    maybeSetProperty(jsonProps, "replyTo",         properties.getReplyTo());
                    maybeSetProperty(jsonProps, "timestamp",       properties.getTimestamp());
                    maybeSetProperty(jsonProps, "type",            properties.getType());
                    maybeSetProperty(jsonProps, "userId",          properties.getUserId());

                    if (jsonProps.size() > 0) {
                        msg.putObject("properties", jsonProps);
                    }
                }

                eb.send(forwardAddress, msg);

                channel.basicAck(deliveryTag, false);
            }
        };

        channel.basicConsume(queueName, cons); // IOException

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

    // {{{ maybeSetProperty
    private void maybeSetProperty(final JsonObject json, final String key, final Object value) {
        if (value != null) {
            if (value instanceof String) {
                json.putString(key, (String) value);
            }
            else if (value instanceof Number) {
                json.putNumber(key, (Number) value);
            }
            else if (value instanceof Date) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) value);

                XMLGregorianCalendar xmlCal =
                    datatypeFactory.newXMLGregorianCalendar(cal);

                json.putString(key, xmlCal.toXMLFormat());
            }
            else {
                throw new IllegalArgumentException("unhandled type " + value.getClass().getName() + " for key " + key);
            }

        }
    }
    // }}}
}
