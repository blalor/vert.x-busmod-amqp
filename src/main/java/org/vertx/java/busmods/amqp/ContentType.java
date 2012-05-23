package org.vertx.java.busmods.amqp;

import java.util.EnumSet;

public enum ContentType {
    APPLICATION_JSON("application/json"),
    APPLICATION_BSON("application/bson"),
    APPLICATION_X_JAVASCRIPT("application/x-javascript"),
    TEXT_JAVASCRIPT("text/javascript"),
    TEXT_X_JAVASCRIPT("text/x-javascript"),
    TEXT_X_JSON("text/x-json"),
    APPLICATION_BINARY("application/binary"),
    TEXT_PLAIN("text/plain");

    public static final EnumSet JSON_CONTENT_TYPES = EnumSet.of(
        APPLICATION_JSON,
        APPLICATION_X_JAVASCRIPT,
        TEXT_JAVASCRIPT,
        TEXT_X_JAVASCRIPT,
        TEXT_X_JSON
    );

    private String contentType;

    // {{{ constructor
    private ContentType(final String contentType) {
        this.contentType = contentType;
    }
    // }}}

    // {{{ getContentType
    public String getContentType() {
        return contentType;
    }
    // }}}

    // {{{ fromString
    public static ContentType fromString(final String contentType) {
        ContentType retVal = null;

        for (ContentType ct : values()) {
            if (ct.contentType.equals(contentType)) {
                retVal = ct;
                break;
            }
        }

        if (retVal == null) {
            throw new IllegalArgumentException("unknown content type " + contentType);
        }

        return retVal;
    }
    // }}}
}
