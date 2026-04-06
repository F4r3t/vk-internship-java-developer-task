package ru.F4r3t.kv.logging;

public record LogField(String key, Object value) {

    public static LogField of(String key, Object value) {
        return new LogField(key, value);
    }
}