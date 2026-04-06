package ru.F4r3t.kv;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.tarantool.client.TarantoolClient;
import ru.F4r3t.kv.config.AppConfig;
import ru.F4r3t.kv.config.TarantoolConfig;
import ru.F4r3t.kv.grpc.KvStoreGrpcService;
import ru.F4r3t.kv.logging.GrpcLoggingInterceptor;
import ru.F4r3t.kv.logging.LogEvent;
import ru.F4r3t.kv.logging.StructuredLogger;
import ru.F4r3t.kv.repository.KvRepository;
import ru.F4r3t.kv.repository.TarantoolKvRepository;
import ru.F4r3t.kv.service.DefaultKvService;
import ru.F4r3t.kv.service.KvService;

import static ru.F4r3t.kv.logging.LogField.of;

public class Application {

    private static final StructuredLogger log = StructuredLogger.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.load();

        log.info(
                LogEvent.APP_CONFIG_LOADED,
                of("grpcPort", config.grpcPort()),
                of("tarantoolHost", config.tarantoolHost()),
                of("tarantoolPort", config.tarantoolPort()),
                of("tarantoolUser", config.tarantoolUser()),
                of("rangePageSize", config.rangePageSize())
        );

        TarantoolClient client = TarantoolConfig.createClient(config);

        KvRepository repository = new TarantoolKvRepository(client);
        KvService kvService = new DefaultKvService(repository);

        Server server = ServerBuilder.forPort(config.grpcPort())
                .addService(ServerInterceptors.intercept(
                        new KvStoreGrpcService(kvService, config.rangePageSize()),
                        new GrpcLoggingInterceptor()
                ))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info(LogEvent.APP_STOP, of("message", "shutdown_hook_triggered"));
            server.shutdown();
            try {
                client.close();
            } catch (Exception e) {
                log.error(LogEvent.APP_STOP, e, of("message", "failed_to_close_tarantool_client"));
            }
        }));

        log.info(
                LogEvent.APP_START,
                of("grpcPort", config.grpcPort()),
                of("tarantoolHost", config.tarantoolHost()),
                of("tarantoolPort", config.tarantoolPort())
        );

        server.awaitTermination();
    }
}