package org.vertx.java.busmods.amqp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.logging.Logger;

import de.undercouch.bson4jackson.BsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

import org.vertx.java.core.json.JsonObject;

import java.util.Map;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

@RunWith(JUnit4.class)
public class TestMessageTransformingConsumer {
    private final Logger logger = Logger.getLogger(getClass().getName());

    private ObjectMapper objectMapper;

    // {{{ setUp
    @Before
    public void setUp() {
        objectMapper = new ObjectMapper(new BsonFactory());
    }
    // }}}

    // {{{ tearDown
    @After
    public void tearDown() {
        objectMapper = null;
    }
    // }}}

    // {{{ decodesBson
    @Test
    public void decodesBson() throws IOException {
        /*
         * equivalent to
         * {
         *     _timestamp=Wed May 23 22:07:01 EDT 2012,
         *     source_addr=[B@10ce7a49,
         *     rf_data=[B@63be573d, (-> "#114:582#")
         *     options=[B@655198f3,
         *     source_addr_long=[B@5010ad7c,
         *     id=[B@76edd0f4
         * }
         */
        byte[] bsonData = new byte[] {
            -126, 0, 0, 0, 9, 95, 116, 105, 109, 101, 115, 116, 97, 109, 112,
            0, -76, -39, -104, 124, 55, 1, 0, 0, 5, 115, 111, 117, 114, 99, 101,
            95, 97, 100, 100, 114, 0, 2, 0, 0, 0, 0, 5, 52, 5, 114, 102, 95,
            100, 97, 116, 97, 0, 11, 0, 0, 0, 0, 35, 49, 49, 52, 58, 53, 56, 50,
            35, 13, 10, 5, 111, 112, 116, 105, 111, 110, 115, 0, 1, 0, 0, 0, 0,
            1, 5, 115, 111, 117, 114, 99, 101, 95, 97, 100, 100, 114, 95, 108,
            111, 110, 103, 0, 8, 0, 0, 0, 0, 0, 19, -94, 0, 64, 58, 91, 10, 5,
            105, 100, 0, 5, 0, 0, 0, 0, 122, 98, 95, 114, 120, 0
        };

        Map bsonMap = objectMapper.readValue(new ByteArrayInputStream(bsonData), Map.class);

        logger.fine(bsonMap.toString());
        logger.fine("rf_data: " + new String((byte[]) bsonMap.get("rf_data")));

        byte[] srcAddrBytes = (byte[]) bsonMap.get("source_addr_long");
        logger.fine("source_addr_long length: " + srcAddrBytes.length); 
        ByteArrayInputStream bais = new ByteArrayInputStream(srcAddrBytes);
        DataInputStream dis = new DataInputStream(bais);

        logger.fine(
            String.format(
                "source_addr: %016x",
                dis.readLong()
            )
        );

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < srcAddrBytes.length; i++) {
            sb.append(String.format("%02X", srcAddrBytes[i]));

            if (i < (srcAddrBytes.length - 1)) {
                sb.append(":");
            }
        }

        logger.fine(sb.toString());

        MessageTransformingConsumer consumer = new MessageTransformingConsumer(null, ContentType.APPLICATION_BINARY) {
            public void doHandle(final String consumerTag,
                                 final Envelope envelope,
                                 final AMQP.BasicProperties properties,
                                 final JsonObject body)
            {
                logger.fine(body.encode());
            }
        };

        Envelope envelope = new Envelope(42L, false, "", "whatever");
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .contentType("application/bson")
            .build();

        consumer.handleDelivery(null, envelope, props, bsonData);
    }
    // }}}
}
