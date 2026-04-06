package ru.F4r3t.kv.service;

import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;

import java.util.Optional;

public interface KvService {
    boolean put(String key, byte[] value);

    Optional<KvEntry> get(String key);

    boolean delete(String key);

    long count();

    RangePage rangePage(String keySince, String keyTo, String afterKey, int limit);
}