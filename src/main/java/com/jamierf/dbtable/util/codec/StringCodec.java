package com.jamierf.dbtable.util.codec;

import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class StringCodec {

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public static final Function<String, byte[]> ENCODER = new Function<String, byte[]>() {
        @Nullable
        @Override
        public byte[] apply(@Nullable String input) {
            return input == null ? new byte[0] : input.getBytes(CHARSET);
        }
    };

    public static final Function<byte[], String> DECODER = new Function<byte[], String>() {
        @Nullable
        @Override
        public String apply(@Nullable byte[] input) {
            return input == null ? "" : new String(input, CHARSET);
        }
    };

    private StringCodec() {}
}
