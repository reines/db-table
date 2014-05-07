package com.jamierf.dbtable.jackson.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.io.IOException;

public class ObjectMapperCodec {

    private final ObjectMapper mapper;

    public ObjectMapperCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public <T> Function<T, byte[]> newEncoder() {
        return new Function<T, byte[]>() {
            @Nullable
            @Override
            public byte[] apply(@Nullable T input) {
                try {
                    return mapper.writeValueAsBytes(input);
                } catch (JsonProcessingException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public <T> Function<byte[], T> newDecoder(final Class<T> type) {
        return new Function<byte[], T>() {
            @Nullable
            @Override
            public T apply(@Nullable byte[] input) {
                try {
                    return mapper.readValue(input, type);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
