package com.jamierf.dbtable.util.container;

import com.google.common.collect.ImmutableMap;
import org.skife.jdbi.v2.ContainerBuilder;

import java.util.Map;

public final class MapContainerBuilder<K, V> implements ContainerBuilder<Map<K, V>> {

    public static <K, V> ContainerBuilder<Map<K, V>> getInstance() {
        return new MapContainerBuilder<>();
    }

    private final ImmutableMap.Builder<K, V> map;

    private MapContainerBuilder() {
        map = ImmutableMap.builder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ContainerBuilder<Map<K, V>> add(Object it) {
        if (!(it instanceof Map.Entry)) {
            throw new IllegalArgumentException("MapContainerFactory can only accept Map.Entry values");
        }

        map.put((Map.Entry<K, V>) it);
        return this;
    }

    @Override
    public Map<K, V> build() {
        return map.build();
    }
}