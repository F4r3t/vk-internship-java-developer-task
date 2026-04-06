package ru.F4r3t.kv.service;

import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;
import ru.F4r3t.kv.repository.KvRepository;

import java.util.Optional;

public final class DefaultKvService implements KvService {

    private final KvRepository repository;

    public DefaultKvService(KvRepository repository) {
        this.repository = repository;
    }

    private static void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
    }

    @Override
    public boolean put(String key, byte[] value) {
        requireKey(key);
        return repository.put(key, value);
    }

    @Override
    public Optional<KvEntry> get(String key) {
        requireKey(key);
        return repository.get(key);
    }

    @Override
    public boolean delete(String key) {
        requireKey(key);
        return repository.delete(key);
    }

    @Override
    public long count() {
        return repository.count();
    }

    @Override
    public RangePage rangePage(String keySince, String keyTo, String afterKey, int limit) {
        if (keySince == null || keyTo == null) {
            throw new IllegalArgumentException("keySince and keyTo must not be null");
        }

        if (keySince.compareTo(keyTo) > 0) {
            throw new IllegalArgumentException("keySince must be <= keyTo");
        }

        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }

        return repository.rangePage(keySince, keyTo, afterKey, limit);
    }
}