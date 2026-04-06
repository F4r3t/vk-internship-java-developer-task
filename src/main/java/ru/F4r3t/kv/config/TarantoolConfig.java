package ru.F4r3t.kv.config;

import io.tarantool.client.TarantoolClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;
import ru.F4r3t.kv.logging.LogEvent;
import ru.F4r3t.kv.logging.StructuredLogger;

import java.util.List;

import static ru.F4r3t.kv.logging.LogField.of;

public final class TarantoolConfig {

    private static final StructuredLogger log = StructuredLogger.getLogger(TarantoolConfig.class);

    private TarantoolConfig() {
    }

    public static TarantoolClient createClient(AppConfig config) throws Exception {
        log.info(
                LogEvent.TARANTOOL_CLIENT_CREATE,
                of("host", config.tarantoolHost()),
                of("port", config.tarantoolPort()),
                of("user", config.tarantoolUser())
        );

        InstanceConnectionGroup.Builder builder = InstanceConnectionGroup.builder()
                .withHost(config.tarantoolHost())
                .withPort(config.tarantoolPort())
                .withUser(config.tarantoolUser())
                .withSize(4);

        if (!"guest".equals(config.tarantoolUser())) {
            builder.withPassword(config.tarantoolPassword());
        }

        InstanceConnectionGroup connectionGroup = builder.build();

        TarantoolClient client = TarantoolFactory.box()
                .withGroups(List.of(connectionGroup))
                .build();

        log.info(
                LogEvent.TARANTOOL_CLIENT_CREATED,
                of("host", config.tarantoolHost()),
                of("port", config.tarantoolPort()),
                of("user", config.tarantoolUser())
        );

        return client;
    }
}