package ru.F4r3t.kv.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import ru.F4r3t.kv.logging.LogEvent;
import ru.F4r3t.kv.logging.StructuredLogger;
import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;
import ru.F4r3t.kv.proto.CountRequest;
import ru.F4r3t.kv.proto.CountResponse;
import ru.F4r3t.kv.proto.DeleteRequest;
import ru.F4r3t.kv.proto.DeleteResponse;
import ru.F4r3t.kv.proto.GetRequest;
import ru.F4r3t.kv.proto.GetResponse;
import ru.F4r3t.kv.proto.KeyValue;
import ru.F4r3t.kv.proto.KvStoreGrpc;
import ru.F4r3t.kv.proto.PutRequest;
import ru.F4r3t.kv.proto.PutResponse;
import ru.F4r3t.kv.proto.RangeRequest;
import ru.F4r3t.kv.service.KvService;

import java.util.Optional;

import static ru.F4r3t.kv.logging.LogField.of;

public final class KvStoreGrpcService extends KvStoreGrpc.KvStoreImplBase {

    private static final StructuredLogger log = StructuredLogger.getLogger(KvStoreGrpcService.class);

    private final KvService kvService;
    private final int defaultPageSize;

    public KvStoreGrpcService(KvService kvService, int defaultPageSize) {
        this.kvService = kvService;
        this.defaultPageSize = defaultPageSize;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;
            boolean overwritten = kvService.put(request.getKey(), value);

            log.info(
                    LogEvent.KV_PUT,
                    of("key", request.getKey()),
                    of("overwritten", overwritten),
                    of("valuePresent", request.hasValue()),
                    of("valueSize", value == null ? 0 : value.length)
            );

            responseObserver.onNext(PutResponse.newBuilder().setOverwritten(overwritten).build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn(
                    LogEvent.RPC_VALIDATION_FAILED,
                    of("method", "Put"),
                    of("key", request.getKey()),
                    of("error", e.getMessage())
            );
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error(
                    LogEvent.RPC_INTERNAL_ERROR,
                    e,
                    of("method", "Put"),
                    of("key", request.getKey())
            );
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            Optional<KvEntry> result = kvService.get(request.getKey());

            if (result.isEmpty()) {
                log.warn(LogEvent.KV_GET_NOT_FOUND, of("key", request.getKey()));
                responseObserver.onError(
                        Status.NOT_FOUND.withDescription("Key not found: " + request.getKey()).asRuntimeException()
                );
                return;
            }

            KvEntry entry = result.get();

            GetResponse.Builder builder = GetResponse.newBuilder().setKey(entry.key());
            if (entry.value() != null) {
                builder.setValue(ByteString.copyFrom(entry.value()));
            }

            log.info(
                    LogEvent.KV_GET,
                    of("key", entry.key()),
                    of("valuePresent", entry.value() != null),
                    of("valueSize", entry.value() == null ? 0 : entry.value().length)
            );

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn(
                    LogEvent.RPC_VALIDATION_FAILED,
                    of("method", "Get"),
                    of("key", request.getKey()),
                    of("error", e.getMessage())
            );
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error(
                    LogEvent.RPC_INTERNAL_ERROR,
                    e,
                    of("method", "Get"),
                    of("key", request.getKey())
            );
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            boolean deleted = kvService.delete(request.getKey());

            log.info(
                    LogEvent.KV_DELETE,
                    of("key", request.getKey()),
                    of("deleted", deleted)
            );

            responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn(
                    LogEvent.RPC_VALIDATION_FAILED,
                    of("method", "Delete"),
                    of("key", request.getKey()),
                    of("error", e.getMessage())
            );
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error(
                    LogEvent.RPC_INTERNAL_ERROR,
                    e,
                    of("method", "Delete"),
                    of("key", request.getKey())
            );
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValue> responseObserver) {
        try {
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : defaultPageSize;
            String afterKey = null;
            int emitted = 0;

            while (true) {
                RangePage page = kvService.rangePage(
                        request.getKeySince(),
                        request.getKeyTo(),
                        afterKey,
                        pageSize
                );

                if (page.items().isEmpty()) {
                    break;
                }

                for (KvEntry entry : page.items()) {
                    KeyValue.Builder builder = KeyValue.newBuilder().setKey(entry.key());
                    if (entry.value() != null) {
                        builder.setValue(ByteString.copyFrom(entry.value()));
                    }
                    responseObserver.onNext(builder.build());
                    emitted++;
                }

                if (page.nextAfter() == null) {
                    break;
                }

                afterKey = page.nextAfter();
            }

            log.info(
                    LogEvent.KV_RANGE,
                    of("from", request.getKeySince()),
                    of("to", request.getKeyTo()),
                    of("pageSize", pageSize),
                    of("emitted", emitted)
            );

            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn(
                    LogEvent.RPC_VALIDATION_FAILED,
                    of("method", "Range"),
                    of("from", request.getKeySince()),
                    of("to", request.getKeyTo()),
                    of("error", e.getMessage())
            );
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error(
                    LogEvent.RPC_INTERNAL_ERROR,
                    e,
                    of("method", "Range"),
                    of("from", request.getKeySince()),
                    of("to", request.getKeyTo())
            );
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = kvService.count();

            log.info(LogEvent.KV_COUNT, of("count", count));

            responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(LogEvent.RPC_INTERNAL_ERROR, e, of("method", "Count"));
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}