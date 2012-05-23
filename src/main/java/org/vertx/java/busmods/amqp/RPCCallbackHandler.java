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
    public RPCCallbackHandler(final Channel channel, final EventBus eb) throws IOException {
        super(channel);

        queueName = channel.queueDeclare().getQueue();
        channel.basicConsume(queueName, this);

        this.eventBus = eb;

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

    // {{{ addMultiResponseCorrelation
    public void addMultiResponseCorrelation(final String correlationId, final String replyTo) {
        correlationMap.put(correlationId, replyTo);
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

        // @todo clean up old correlations
        Object target = correlationMap.get(correlationId);
        if (target == null) {
            logger.warning("No message to reply to for " + correlationId);
        } else {
            if (target instanceof Message) {
                Message<JsonObject> msg = (Message<JsonObject>) target;
                correlationMap.remove(correlationId);

                msg.reply(body);
            } else {
                String replyTo = (String) target;

                eventBus.send(replyTo, body);
            }
        }
    }
    // }}}
}
