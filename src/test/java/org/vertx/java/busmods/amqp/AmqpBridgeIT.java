package org.vertx.java.busmods.amqp;

import org.vertx.java.framework.TestBase;

public class AmqpBridgeIT extends TestBase implements AmqpBridgeTests {
    // {{{ setUp
    /** {@inheritDoc} */
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        startApp(AmqpBridgeTestClient.class.getName());
    }
    // }}}

    // {{{ testFoo
    /** {@inheritDoc} */
    public void testFoo() {
        startTest(getMethodName());
    }
    // }}}
}
