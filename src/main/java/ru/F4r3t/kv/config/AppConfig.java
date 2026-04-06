package ru.F4r3t.kv.config;

import java.io.InputStream;
import java.util.Properties;

public record AppConfig(
        String tarantoolHost,
        int tarantoolPort,
        String tarantoolUser,
        String tarantoolPassword,
        int grpcPort,
        int rangePageSize
) {
    public static AppConfig load() {
        Properties properties = new Properties();

        try (InputStream inputStream = AppConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }

        return new AppConfig(
                getString(properties, "tarantool.host", "TT_HOST", "localhost"),
                getInt(properties, "tarantool.port", "TT_PORT", 3301),
                getString(properties, "tarantool.user", "TT_USER", "guest"),
                getString(properties, "tarantool.password", "TT_PASSWORD", ""),
                getInt(properties, "grpc.port", "GRPC_PORT", 9090),
                getInt(properties, "range.page-size", "RANGE_PAGE_SIZE", 1000)
        );
    }

    private static String getString(Properties properties, String propertyKey, String envKey, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        return properties.getProperty(propertyKey, defaultValue);
    }

    private static int getInt(Properties properties, String propertyKey, String envKey, int defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return Integer.parseInt(envValue);
        }

        String propertyValue = properties.getProperty(propertyKey);
        if (propertyValue == null || propertyValue.isBlank()) {
            return defaultValue;
        }

        return Integer.parseInt(propertyValue);
    }
}