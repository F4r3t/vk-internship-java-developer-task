package ru.F4r3t.kv.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringJoiner;

public final class StructuredLogger {

    private final Logger logger;

    private StructuredLogger(Class<?> type) {
        this.logger = LoggerFactory.getLogger(type);
    }

    public static StructuredLogger getLogger(Class<?> type) {
        return new StructuredLogger(type);
    }

    public void info(LogEvent event, LogField... fields) {
        if (logger.isInfoEnabled()) {
            logger.info(format(event, fields));
        }
    }

    public void debug(LogEvent event, LogField... fields) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(event, fields));
        }
    }

    public void warn(LogEvent event, LogField... fields) {
        if (logger.isWarnEnabled()) {
            logger.warn(format(event, fields));
        }
    }

    public void error(LogEvent event, Throwable throwable, LogField... fields) {
        if (logger.isErrorEnabled()) {
            logger.error(format(event, fields), throwable);
        }
    }

    private String format(LogEvent event, LogField... fields) {
        StringJoiner joiner = new StringJoiner(" ");
        joiner.add("event=" + event.code());
        joiner.add("requestId=" + sanitize(RequestContext.requestId()));

        for (LogField field : fields) {
            joiner.add(field.key() + "=" + sanitize(field.value()));
        }

        return joiner.toString();
    }

    private String sanitize(Object value) {
        if (value == null) {
            return "null";
        }

        String text = String.valueOf(value);
        text = text.replace("\\", "\\\\").replace("\"", "\\\"");
        if (text.contains(" ") || text.contains("=")) {
            return "\"" + text + "\"";
        }
        return text;
    }
}