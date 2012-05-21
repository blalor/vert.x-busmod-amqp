// ==========
// with handler, standard request/response
var rpc_request = {
    // "exchange": '', // optional
    "routingKey": "xbee_tx",
    "body": { … }
};

eb.send("rabbit.invoke_rpc", rpc_request, function(response) {
    /*
    { … }
    */

    // handle response
});

// ==========
// without handler but with replyTo, multiple responses to result queue
/**
 * Handler for RPC response messages.
 */
var pending_requests = {};

// handler for multiple RPC responses
var rpc_response_handler_id = genUniqueId();

eb.registerHandler(rpc_response_handler_id, function(message) {
    /*
    {
        exchange: String,
        routingKey: String,
        responseComplete: false,
        properties: {
            correlationId   : …,
        },
        body: { … }
    }
    */
    var correlationId = message.properties.correlationId;
    var pending_request = pending_requests[correlationId];

    // handle the response

    if (message.responseComplete) {
        // close pending request

        delete pending_requests[correlationId];
    }
});

var correlationId = genUniqueId();
pending_requests[correlationId] = { … };

var rpc_request = {
    // "exchange": "", // 
    "routingKey": "xbee_tx",
    "properties": {
        "correlationId" : correlationId,
        "replyTo" : rpc_response_handler_id,
    },
    "body": { … }
};

eb.send("rabbit.invoke_rpc", rpc_request);
