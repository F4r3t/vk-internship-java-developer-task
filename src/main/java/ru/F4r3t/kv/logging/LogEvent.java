package ru.F4r3t.kv.logging;

public enum LogEvent {
    APP_START("app_start"),
    APP_STOP("app_stop"),
    APP_CONFIG_LOADED("app_config_loaded"),

    TARANTOOL_CLIENT_CREATE("tarantool_client_create"),
    TARANTOOL_CLIENT_CREATED("tarantool_client_created"),
    TARANTOOL_CALL_FAILED("tarantool_call_failed"),

    RPC_STARTED("rpc_started"),
    RPC_FINISHED("rpc_finished"),
    RPC_VALIDATION_FAILED("rpc_validation_failed"),
    RPC_INTERNAL_ERROR("rpc_internal_error"),

    KV_PUT("kv_put"),
    KV_GET("kv_get"),
    KV_GET_NOT_FOUND("kv_get_not_found"),
    KV_DELETE("kv_delete"),
    KV_RANGE("kv_range"),
    KV_COUNT("kv_count");

    private final String code;

    LogEvent(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}