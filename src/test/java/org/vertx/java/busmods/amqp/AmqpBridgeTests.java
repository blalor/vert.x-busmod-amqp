package org.vertx.java.busmods.amqp;

public interface AmqpBridgeTests {
    void testFoo();
    void testInvokeRpcWithSingleReply();
    void testInvokeRpcWithMultipleReplies();
    void testSendBsonToAMQP();
}
