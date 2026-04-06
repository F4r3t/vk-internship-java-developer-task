package ru.F4r3t.kv.repository;

import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;

import java.util.Optional;

public interface KvRepository {
    boolean put(String key, byte[] value);

    Optional<KvEntry> get(String key);

    boolean delete(String key);

    long count();

    RangePage rangePage(String keySince, String keyTo, String afterKey, int limit);
}