package org.vertx.java.busmods.amqp;

import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.LongString;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;

import java.util.Date;
import java.util.GregorianCalendar;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;

import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.json.DecodeException;

import de.undercouch.bson4jackson.BsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * {@link com.rabbitmq.client.Consumer} implementation that transforms messages
 * to {@link JsonObject}s.
 *
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 */
abstract class MessageTransformingConsumer extends DefaultConsumer {
    private final Logger logger = Logger.getLogger(getClass().getName());

    private ContentType defaultContentType;

    /**
     * This may or may not be thread safe.  Here goes nothin'…
     */
    private final DatatypeFactory datatypeFactory;

    private final BsonFactory bsonFactory = new BsonFactory();

    // {{{ constructor
    public MessageTransformingConsumer(final Channel channel, final ContentType defaultContentType) {
        super(channel);

        this.defaultContentType = defaultContentType;

        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException("unable to get datatype factory", e);
        }
    }
    // }}}

    /**
     * Must be provided by concrete implementation.
     */
    public abstract void doHandle(final String consumerTag,
                                  final Envelope envelope,
                                  final AMQP.BasicProperties properties,
                                  final JsonObject body)
        throws IOException;

    // {{{ handleDelivery
    /** {@inheritDoc} */
    public void handleDelivery(final String consumerTag,
                               final Envelope envelope,
                               final AMQP.BasicProperties properties,
                               final byte[] body)
        throws IOException
    {
        /*
            {
                exchange: String,
                routingKey: String,
                properties: { … },
                body: { … }
            }
        */
        JsonObject msg = new JsonObject()
            .putString("exchange", envelope.getExchange())
            .putString("routingKey", envelope.getRoutingKey());

        JsonObject jsonProps = new JsonObject();
        msg.putObject("properties", jsonProps);

        if (properties != null) {
            maybeSetProperty(jsonProps, "appId",           properties.getAppId());

            // I think these will always be "basic"
            // maybeSetProperty(jsonProps, "classId",         properties.getClassId());
            // maybeSetProperty(jsonProps, "className",       properties.getClassName());

            maybeSetProperty(jsonProps, "clusterId",       properties.getClusterId());
            maybeSetProperty(jsonProps, "contentEncoding", properties.getContentEncoding());
            maybeSetProperty(jsonProps, "correlationId",   properties.getCorrelationId());
            maybeSetProperty(jsonProps, "deliveryMode",    properties.getDeliveryMode());
            maybeSetProperty(jsonProps, "expiration",      properties.getExpiration());

            if (properties.getHeaders() != null) {
                JsonObject headersObj = new JsonObject();
                jsonProps.putObject("headers", headersObj);

                for (Map.Entry<String,Object> entry : properties.getHeaders().entrySet()) {
                    maybeSetProperty(headersObj, entry.getKey(), entry.getValue());
                }
            }

            maybeSetProperty(jsonProps, "messageId", properties.getMessageId());
            maybeSetProperty(jsonProps, "priority",  properties.getPriority());
            maybeSetProperty(jsonProps, "replyTo",   properties.getReplyTo());
            maybeSetProperty(jsonProps, "timestamp", properties.getTimestamp());
            maybeSetProperty(jsonProps, "type",      properties.getType());
            maybeSetProperty(jsonProps, "userId",    properties.getUserId());
        }

        ContentType contentType = defaultContentType;

        try {
            contentType = ContentType.fromString(properties.getContentType());
        } catch (IllegalArgumentException e) {
            logger.warning(
                "unknown content type " + properties.getContentType() +
                "; defaulting to " + contentType.getContentType()
            );
        }

        // attempt to decode content by content type
        boolean decoded = false;
        try {
            if (ContentType.JSON_CONTENT_TYPES.contains(contentType)) {
                msg.putObject("body", new JsonObject(new String(body)));

                decoded = true;
                contentType = ContentType.APPLICATION_JSON;
            }
            else if (ContentType.APPLICATION_BSON == contentType) {
                ObjectMapper om = new ObjectMapper(bsonFactory);
                Map<String,Object> data = om.readValue(new ByteArrayInputStream(body), Map.class);

                msg.putObject("body", new JsonObject(data));

                decoded = true;
            }
            else if (ContentType.TEXT_PLAIN == contentType) {
                msg.putString(
                    "body",
                    new String(body, jsonProps.getString("contentEncoding", "UTF-8"))
                );

                decoded = true;
            }
        } catch (DecodeException e) {
            logger.log(Level.WARNING, "Unable to decode message body as " + contentType, e);
        } catch (UnsupportedEncodingException e) {
            logger.log(Level.WARNING, "Unsupported encoding decoding body", e);
        } finally {
            if (! decoded) {
                contentType = ContentType.APPLICATION_BINARY;
                logger.warning("storing body as " + contentType);

                msg.putBinary("body", body);
            }
        }

        jsonProps.putString("contentType", contentType.getContentType());

        // delegate handling of transformed message
        doHandle(consumerTag, envelope, properties, msg);
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
            else if (value instanceof JsonObject) {
                json.putObject(key, (JsonObject) value);
            }
            else if (value instanceof LongString) {
                try {
                    json.putString(
                        key,
                        new String(((LongString) value).getBytes(), "UTF-8")
                    );
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalStateException("UTF-8 is not supported, eh?  Really?", e);
                }
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
