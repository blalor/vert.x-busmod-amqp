package org.vertx.java.busmods.amqp;

import com.rabbitmq.client.DefaultConsumer;
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

import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Class for managing and dispatching RPC responses to their appropriate
 * EventBus handler.
 *
 * Based on the description of {@link DefaultConsumer}, this should never be
 * executed by more than one thread at a time.
 *
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 */
public class RPCCallbackHandler extends DefaultConsumer {
    private final Logger logger = LoggerFactory.getLogger(getClass());

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
    public RPCCallbackHandler(final Channel channel, final EventBus eb) throws IOException {
        super(channel);

        queueName = channel.queueDeclare().getQueue();
        channel.basicConsume(queueName, this);

        this.eventBus = eb;

        logger.trace("initialization complete, with queue " + queueName);
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
    public void addMultiResponseCorrelation(final String correlationId, final String replyTo) {
        correlationMap.put(correlationId, replyTo);
    }
    // }}}

    // {{{ handleDelivery
    /**
     * Dispatch the RPC response back to the invoker.
     */
    @Override
    public void handleDelivery(
        final String consumerTag,
        final Envelope envelope,
        final AMQP.BasicProperties properties,
        final byte[] body
    )
        throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();

        // I don't forsee being able to recover if we can't process this message
        getChannel().basicAck(deliveryTag, false);

        String correlationId = properties.getCorrelationId();

        Object target = correlationMap.get(correlationId);
        if (target == null) {
            logger.warn("No message to reply to for " + correlationId);
        } else {
            JsonObject ebReply = new JsonObject(new String(body, "UTF-8"));

            if (target instanceof Message) {
                Message<JsonObject> msg = (Message<JsonObject>) target;
                correlationMap.remove(correlationId);

                msg.reply(ebReply);
            } else {
                String replyTo = (String) target;

                eventBus.send(replyTo, ebReply);
            }
        }
    }
    // }}}
}
