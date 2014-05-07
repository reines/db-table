package com.jamierf.dbtable.core.util;

import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public final class StringCodec {

    public static final Function<String, byte[]> ENCODER = new Function<String, byte[]>() {
        @Nullable
        @Override
        public byte[] apply(@Nullable String input) {
            return input == null ? new byte[0] : input.getBytes(StandardCharsets.UTF_8);
        }
    };

    public static final Function<byte[], String> DECODER = new Function<byte[], String>() {
        @Nullable
        @Override
        public String apply(@Nullable byte[] input) {
            return new String(input == null ? new byte[0] : input, StandardCharsets.UTF_8);
        }
    };

    private StringCodec() {}
}
