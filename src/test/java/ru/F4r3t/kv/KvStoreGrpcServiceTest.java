package ru.F4r3t.kv;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.F4r3t.kv.grpc.KvStoreGrpcService;
import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;
import ru.F4r3t.kv.proto.CountRequest;
import ru.F4r3t.kv.proto.CountResponse;
import ru.F4r3t.kv.proto.DeleteRequest;
import ru.F4r3t.kv.proto.DeleteResponse;
import ru.F4r3t.kv.proto.GetRequest;
import ru.F4r3t.kv.proto.GetResponse;
import ru.F4r3t.kv.proto.KeyValue;
import ru.F4r3t.kv.proto.PutRequest;
import ru.F4r3t.kv.proto.PutResponse;
import ru.F4r3t.kv.proto.RangeRequest;
import ru.F4r3t.kv.service.KvService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KvStoreGrpcServiceTest {

    @Mock
    private KvService kvService;

    private KvStoreGrpcService grpcService;

    @BeforeEach
    void setUp() {
        grpcService = new KvStoreGrpcService(kvService, 2);
    }

    @Test
    void putShouldHandleNullValue() {
        when(kvService.put("k1", null)).thenReturn(false);
        TestObserver<PutResponse> observer = new TestObserver<>();

        grpcService.put(PutRequest.newBuilder().setKey("k1").build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.values.size());
        assertFalse(observer.values.getFirst().getOverwritten());
    }

    @Test
    void getShouldReturnResponseForExistingKey() {
        when(kvService.get("k2")).thenReturn(Optional.of(new KvEntry("k2", new byte[] {4, 5})));
        TestObserver<GetResponse> observer = new TestObserver<>();

        grpcService.get(GetRequest.newBuilder().setKey("k2").build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.values.size());
        GetResponse response = observer.values.getFirst();
        assertEquals("k2", response.getKey());
        assertTrue(response.hasValue());
        assertArrayEquals(new byte[] {4, 5}, response.getValue().toByteArray());
    }

    @Test
    void getShouldReturnNotFoundForMissingKey() {
        when(kvService.get("missing")).thenReturn(Optional.empty());
        TestObserver<GetResponse> observer = new TestObserver<>();

        grpcService.get(GetRequest.newBuilder().setKey("missing").build(), observer);

        assertNotNull(observer.error);
        StatusRuntimeException exception = (StatusRuntimeException) observer.error;
        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void deleteShouldReturnDeletedFlag() {
        when(kvService.delete("k3")).thenReturn(true);
        TestObserver<DeleteResponse> observer = new TestObserver<>();

        grpcService.delete(DeleteRequest.newBuilder().setKey("k3").build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.values.size());
        assertTrue(observer.values.getFirst().getDeleted());
    }

    @Test
    void rangeShouldStreamAllPages() {
        when(kvService.rangePage("a", "z", null, 2))
                .thenReturn(new RangePage(List.of(
                        new KvEntry("a", new byte[] {1}),
                        new KvEntry("b", null)
                ), "b"));
        when(kvService.rangePage("a", "z", "b", 2))
                .thenReturn(new RangePage(List.of(
                        new KvEntry("c", new byte[] {3})
                ), null));

        TestObserver<KeyValue> observer = new TestObserver<>();
        grpcService.range(RangeRequest.newBuilder().setKeySince("a").setKeyTo("z").build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(3, observer.values.size());
        assertEquals("a", observer.values.get(0).getKey());
        assertTrue(observer.values.get(0).hasValue());
        assertFalse(observer.values.get(1).hasValue());
        assertEquals("c", observer.values.get(2).getKey());
    }

    @Test
    void countShouldReturnCountValue() {
        when(kvService.count()).thenReturn(7L);
        TestObserver<CountResponse> observer = new TestObserver<>();

        grpcService.count(CountRequest.newBuilder().build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.values.size());
        assertEquals(7L, observer.values.getFirst().getCount());
    }

    @Test
    void putShouldMapIllegalArgumentExceptionToInvalidArgument() {
        when(kvService.put("", null)).thenThrow(new IllegalArgumentException("bad key"));
        TestObserver<PutResponse> observer = new TestObserver<>();

        grpcService.put(PutRequest.newBuilder().setKey("").build(), observer);

        assertNotNull(observer.error);
        StatusRuntimeException exception = (StatusRuntimeException) observer.error;
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals("bad key", exception.getStatus().getDescription());
    }

    private static final class TestObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
