package io.orderedoutbox.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.orderedoutbox.domain.EventPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class EventPayloadCodec {

    @Inject
    ObjectMapper objectMapper;

    public EventPayload decode(String json) {
        try {
            return objectMapper.readValue(json, EventPayload.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("undecodable order event", exception);
        }
    }

    public String encode(EventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("could not encode order event", exception);
        }
    }
}
