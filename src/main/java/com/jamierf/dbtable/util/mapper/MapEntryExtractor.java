package com.jamierf.dbtable.util.mapper;

import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.util.Map;

public final class MapEntryExtractor {

    public static <K, V> Function<Map.Entry<K, V>, K> getKey() {
        return new Function<Map.Entry<K, V>, K>() {
            @Nullable
            @Override
            public K apply(@Nullable Map.Entry<K, V> input) {
                return input == null ? null : input.getKey();
            }
        };
    }

    public static <K, V> Function<Map.Entry<K, V>, V> getValue() {
        return new Function<Map.Entry<K, V>, V>() {
            @Nullable
            @Override
            public V apply(@Nullable Map.Entry<K, V> input) {
                return input == null ? null : input.getValue();
            }
        };
    }

    private MapEntryExtractor() {}
}
