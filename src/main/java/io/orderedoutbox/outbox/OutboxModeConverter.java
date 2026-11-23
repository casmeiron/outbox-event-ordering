package io.orderedoutbox.outbox;

import org.eclipse.microprofile.config.spi.Converter;

import java.util.Locale;

/** Accepts {@code safe} and {@code naive} as well as the enum names. */
public class OutboxModeConverter implements Converter<OutboxMode> {

    @Override
    public OutboxMode convert(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("outbox.mode is required");
        }
        return OutboxMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
