package ru.F4r3t.kv.logging;

import io.grpc.Context;
import io.grpc.ForwardingServerCall;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.Contexts;

import java.util.UUID;

import static ru.F4r3t.kv.logging.LogField.of;

public final class GrpcLoggingInterceptor implements ServerInterceptor {

    private static final StructuredLogger log = StructuredLogger.getLogger(GrpcLoggingInterceptor.class);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        String requestId = UUID.randomUUID().toString();
        String method = call.getMethodDescriptor().getFullMethodName();
        String remote = String.valueOf(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
        long startedAt = System.nanoTime();

        Context context = Context.current().withValue(RequestContext.REQUEST_ID, requestId);

        log.info(
                LogEvent.RPC_STARTED,
                of("method", method),
                of("remote", remote)
        );

        ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;

                if (status.isOk()) {
                    log.info(
                            LogEvent.RPC_FINISHED,
                            of("method", method),
                            of("status", status.getCode()),
                            of("elapsedMs", elapsedMs)
                    );
                } else {
                    log.warn(
                            LogEvent.RPC_FINISHED,
                            of("method", method),
                            of("status", status.getCode()),
                            of("description", status.getDescription()),
                            of("elapsedMs", elapsedMs)
                    );
                }

                super.close(status, trailers);
            }
        };

        return Contexts.interceptCall(context, wrappedCall, headers, next);
    }
}