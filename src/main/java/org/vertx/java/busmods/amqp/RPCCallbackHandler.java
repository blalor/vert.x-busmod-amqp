package org.vertx.java.busmods.amqp;

import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.logging.Logger;

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
    private final Logger logger = Logger.getLogger(getClass().getName());

    /**
     * Maintains relationship between correlationIds and their intended targets.
     */
    private final Map<String,Message<JsonObject>> correlationMap = 
        Collections.synchronizedMap(new HashMap<String,Message<JsonObject>>());

    /**
     * Name of internal queue for receiving RPC responses.
     */
    private final String queueName;

    // {{{ constructor
    public RPCCallbackHandler(final Channel channel) throws IOException {
        super(channel);

        queueName = channel.queueDeclare().getQueue();
        channel.basicConsume(queueName, this);

        logger.finest("initialization complete, with queue " + queueName);
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

        String correlationId = properties.getCorrelationId();

        Message<JsonObject> msg = correlationMap.remove(correlationId);
        getChannel().basicAck(deliveryTag, false);

        if (msg == null) {
            logger.warning("No message to reply to for " + correlationId);
        }
        else {
            msg.reply(new JsonObject(new String(body)));
        }
    }
    // }}}
}
