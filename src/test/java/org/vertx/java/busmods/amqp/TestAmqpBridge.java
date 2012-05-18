package org.vertx.java.busmods.amqp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;

import org.easymock.Capture;

import static org.junit.Assert.*;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.Handler;
import org.vertx.java.deploy.Container;

import org.vertx.java.core.json.JsonObject;

import org.vertx.java.core.eventbus.EventBus;

import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

@RunWith(JUnit4.class)
public class TestAmqpBridge {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private AmqpBridge verticle;

    private JsonObject defaultJsonConfig;

    private Vertx mockVertx;
    private Container mockContainer;
    private EventBus mockEventBus;

    // {{{ setUp
    @Before
    public void setUp() {
        defaultJsonConfig = new JsonObject();
        defaultJsonConfig.putString("uri", "amqp://localhost");
        defaultJsonConfig.putString("address", "test.address");

        mockEventBus = createMock(EventBus.class);

        mockVertx = createMock(Vertx.class);
        expect(mockVertx.eventBus())
            .andReturn(mockEventBus)
            .anyTimes();

        mockContainer = createMock(Container.class);
        expect(mockContainer.getConfig())
            .andReturn(defaultJsonConfig);

        expect(mockContainer.getLogger())
            .andReturn(LoggerFactory.getLogger("mockContainer"))
            .anyTimes();

        verticle = new AmqpBridge();
        verticle.setVertx(mockVertx);
        verticle.setContainer(mockContainer);
    }
    // }}}

    // {{{ tearDown
    @After
    public void tearDown() {
        defaultJsonConfig = null;

        mockEventBus = null;
        
        verticle = null;
        mockContainer = null;
        mockVertx = null;
    }
    // }}}

    // {{{ configExpectsAddressAndUrl
    @Test(expected = IllegalArgumentException.class)
    public void configExpectsAddressAndUrl() throws Exception {
        defaultJsonConfig.removeField("uri");
        defaultJsonConfig.removeField("address");

        replay(mockContainer, mockVertx, mockEventBus);

        verticle.start();

        verify(mockContainer, mockVertx, mockEventBus);
    }
    // }}}
}
