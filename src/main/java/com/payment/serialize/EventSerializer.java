package com.payment.serialize;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
public class EventSerializer {

    private final ObjectMapper mapper;

    public Json toJson(Object event) {
        try {
            String jsonString = mapper.writeValueAsString(event);
            return Json.of(jsonString);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event to JSON", e);
        }
    }

    public <T> T fromJson(Json json, Class<T> clazz) {
        return fromJson(json.toString(), clazz);
    }

    public <T> T fromJson(String json, Class<T> clazz) {
        try {
            if (json == null) {
                return null;
            }
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON to " + clazz.getSimpleName(), e);
        }
    }
}