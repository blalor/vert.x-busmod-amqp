package org.vertx.java.busmods.amqp;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Class for managing and dispatching RPC responses to their appropriate
 * EventBus handler.
 *
 * Based on the description of {@link com.rabbitmq.client.DefaultConsumer}, this should never be
 * executed by more than one thread at a time.
 *
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 */
public class RPCCallbackHandler extends MessageTransformingConsumer {
    private class MultiResponseCorrelation {
        /**
         * Default time-to-live of 10 minutes.
         */
        private static final int DEFAULT_TTL = 10;

        public final String eventBusCorrelationId;
        public final String eventBusReplyToAddress;
        private int ttl = DEFAULT_TTL;
        private long expiration;

        public MultiResponseCorrelation(final String correlationId,
                                        final String replyTo,
                                        final Integer ttl
        ) {
            this.eventBusCorrelationId = correlationId;
            this.eventBusReplyToAddress = replyTo;

            if (ttl != null) {
                this.ttl = ttl;
            }

            updateExpiration();
        }

        // {{{ updateExpiration
        public void updateExpiration() {
            expiration = System.currentTimeMillis() + (ttl * 60 * 1000);
        }
        // }}}

        // {{{ isExpired
        public boolean isExpired() {
            return System.currentTimeMillis() > expiration;
        }
        // }}}
    }

    private final Logger logger = Logger.getLogger(getClass().getName());

    /**
     * Maintains relationship between correlationIds and their intended targets.
     */
    private final Map<String,Object> correlationMap = 
        Collections.synchronizedMap(new HashMap<String,Object>());

    /**
     * Name of internal queue for receiving RPC responses.
     */
    private final String queueName;

    /**
     * Instance of the EventBus.
     */
    private EventBus eventBus;

    // {{{ constructor
    public RPCCallbackHandler(final Channel channel,
                              final ContentType defaultContentType,
                              final EventBus eb)
        throws IOException
    {
        super(channel, defaultContentType);

        queueName = channel.queueDeclare().getQueue();
        channel.basicConsume(queueName, this);

        this.eventBus = eb;
    }
    // }}}

    // {{{ getQueueName
    /** 
     * Getter for queueName.
     *
     * @return value for queueName
     */
    public String getQueueName() {
        return queueName;
    }
    // }}}

    // {{{ addCorrelation
    public void addCorrelation(final String correlationId, final Message<JsonObject> message) {
        correlationMap.put(correlationId, message);
    }
    // }}}

    // {{{ addMultiResponseCorrelation
    /**
     * @param correlationId The AMQP correlation property
     * @param eventBusCorrelationId The correlationId provided by the EventBus sender
     * @param replyTo the EventBus address to send replies to
     * @param timeToLive duration of correlation after last received message
     */
    public void addMultiResponseCorrelation(final String correlationId,
                                            final String eventBusCorrelationId,
                                            final String replyTo,
                                            final Integer timeToLive
    ) {
        correlationMap.put(
            correlationId,
            new MultiResponseCorrelation(eventBusCorrelationId, replyTo, timeToLive)
        );
    }
    // }}}

    // {{{ doHandle
    /**
     * Dispatch the RPC response back to the invoker.
     */
    @Override
    public void doHandle(final String consumerTag,
                         final Envelope envelope,
                         final AMQP.BasicProperties properties,
                         final JsonObject body)
        throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();

        // I don't forsee being able to recover if we can't process this message
        getChannel().basicAck(deliveryTag, false);

        // remove some elements of the message that don't apply to the recipient
        body.removeField("exchange");
        body.removeField("routingKey");
        body.getObject("properties").removeField("correlationId");

        String correlationId = properties.getCorrelationId();

        Object target = correlationMap.get(correlationId);
        if (target == null) {
            logger.warning("No message to reply to for " + correlationId);
        } else {
            if (target instanceof Message) {
                Message<JsonObject> msg = (Message<JsonObject>) target;
                correlationMap.remove(correlationId);

                msg.reply(body);
            } else {
                MultiResponseCorrelation mrc = (MultiResponseCorrelation) target;
                mrc.updateExpiration();

                if (mrc.eventBusCorrelationId != null) {
                    body.getObject("properties").putString("correlationId", mrc.eventBusCorrelationId);
                }

                eventBus.send(mrc.eventBusReplyToAddress, body);
            }
        }

        // clean up old correlations
        for (String cid : correlationMap.keySet()) {
            Object o = correlationMap.get(cid);

            if (o instanceof MultiResponseCorrelation) {
                if (((MultiResponseCorrelation) o).isExpired()) {
                    logger.finer("removing expired correlation " + cid);

                    correlationMap.remove(cid);
                }
            }
        }
    }
    // }}}
}
