package ru.F4r3t.kv.logging;

import io.grpc.Context;

public final class RequestContext {

    public static final Context.Key<String> REQUEST_ID = Context.key("requestId");

    private RequestContext() {
    }

    public static String requestId() {
        String value = REQUEST_ID.get();
        return value == null ? "-" : value;
    }
}