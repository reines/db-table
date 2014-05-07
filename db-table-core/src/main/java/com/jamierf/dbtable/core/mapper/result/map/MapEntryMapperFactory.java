package com.jamierf.dbtable.core.mapper.result.map;

import com.jamierf.dbtable.core.mapper.result.field.FieldMapperFactory;

public class MapEntryMapperFactory<K, V> {

    private final FieldMapperFactory<K> keyMapperFactory;
    private final FieldMapperFactory<V> valueMapperFactory;

    public MapEntryMapperFactory(FieldMapperFactory<K> keyMapperFactory, FieldMapperFactory<V> valueMapperFactory) {
        this.keyMapperFactory = keyMapperFactory;
        this.valueMapperFactory = valueMapperFactory;
    }

    public MapEntryMapper<K, V> build(String keyFieldName, String valueFieldName) {
        return new MapEntryMapper<>(
                keyMapperFactory.build(keyFieldName),
                valueMapperFactory.build(valueFieldName)
        );
    }
}
