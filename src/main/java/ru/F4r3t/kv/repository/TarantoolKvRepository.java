package ru.F4r3t.kv.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import io.tarantool.client.TarantoolClient;
import ru.F4r3t.kv.logging.LogEvent;
import ru.F4r3t.kv.logging.StructuredLogger;
import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static ru.F4r3t.kv.logging.LogField.of;

public final class TarantoolKvRepository implements KvRepository {

    private static final StructuredLogger log = StructuredLogger.getLogger(TarantoolKvRepository.class);

    private final TarantoolClient client;

    public TarantoolKvRepository(TarantoolClient client) {
        this.client = client;
    }

    @Override
    public boolean put(String key, byte[] value) {
        Map<String, Object> result = callSingleMap("kv_put", args(key, value));
        return Boolean.TRUE.equals(result.get("overwritten"));
    }

    @Override
    public Optional<KvEntry> get(String key) {
        Map<String, Object> result = callSingleMap("kv_get", args(key));
        boolean found = Boolean.TRUE.equals(result.get("found"));
        if (!found) {
            return Optional.empty();
        }

        String returnedKey = (String) result.get("key");
        byte[] value = toBytes(result.get("value"));
        return Optional.of(new KvEntry(returnedKey, value));
    }

    @Override
    public boolean delete(String key) {
        Map<String, Object> result = callSingleMap("kv_delete", args(key));
        return Boolean.TRUE.equals(result.get("deleted"));
    }

    @Override
    public long count() {
        Map<String, Object> result = callSingleMap("kv_count", args());
        Object count = result.get("count");
        if (count instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException("Unexpected count response: " + count);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RangePage rangePage(String keySince, String keyTo, String afterKey, int limit) {
        Map<String, Object> result = callSingleMap(
                "kv_range_page",
                args(keySince, keyTo, afterKey, limit)
        );

        List<Map<String, Object>> rawItems =
                (List<Map<String, Object>>) result.getOrDefault("items", List.of());

        List<KvEntry> items = new ArrayList<>(rawItems.size());
        for (Map<String, Object> rawItem : rawItems) {
            String key = (String) rawItem.get("key");
            byte[] value = toBytes(rawItem.get("value"));
            items.add(new KvEntry(key, value));
        }

        String nextAfter = result.get("next_after") == null
                ? null
                : String.valueOf(result.get("next_after"));

        return new RangePage(items, nextAfter);
    }

    private Map<String, Object> callSingleMap(String functionName, List<Object> arguments) {
        try {
            List<Map<String, Object>> response = client
                    .call(functionName, arguments, new TypeReference<List<Map<String, Object>>>() {})
                    .join()
                    .get();

            if (response == null || response.isEmpty()) {
                throw new IllegalStateException("Empty response from Tarantool function: " + functionName);
            }

            return response.getFirst();
        } catch (Exception e) {
            log.error(
                    LogEvent.TARANTOOL_CALL_FAILED,
                    e,
                    of("function", functionName),
                    of("argumentsCount", arguments.size())
            );
            throw new RuntimeException("Tarantool call failed: " + functionName, e);
        }
    }

    private static List<Object> args(Object... values) {
        return new ArrayList<>(Arrays.asList(values));
    }

    private static byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[] bytes) {
            return bytes;
        }

        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }

        if (value instanceof List<?> list) {
            byte[] bytes = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                if (!(element instanceof Number number)) {
                    throw new IllegalStateException("Unsupported binary element: " + element);
                }
                bytes[i] = number.byteValue();
            }
            return bytes;
        }

        throw new IllegalStateException("Unsupported binary type: " + value.getClass());
    }
}