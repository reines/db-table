package com.jamierf.dbtable.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.jamierf.dbtable.mapper.MapEntryExtractor;

import java.util.Collection;
import java.util.Map;

public class MapEntrySelectionMapFactory<K, V> extends AbstractSelectionMapFactory<Map.Entry<K, V>> {

    private final String keyFieldName;
    private final String valueFieldName;

    private final Function<Map.Entry<K, V>, K> keyExtractor;
    private final Function<Map.Entry<K, V>, V> valueExtrator;

    public MapEntrySelectionMapFactory(String keyFieldName, String valueFieldName) {
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;

        keyExtractor = MapEntryExtractor.getKey();
        valueExtrator = MapEntryExtractor.getValue();
    }

    @Override
    public Collection<String> keyFields() {
        return ImmutableSet.of(keyFieldName, valueFieldName);
    }

    @Override
    public SelectionMap getSelectionMap(Map.Entry<K, V> value) {
        return new SelectionMap(ImmutableMap.of(
                keyFieldName, value.getKey(),
                valueFieldName, value.getValue()
        ));
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<Map.Entry<K, V>> value) {
        return new SelectionMap(ImmutableMap.<String, Object>of(
                keyFieldName, Iterables.transform(value, keyExtractor),
                valueFieldName, Iterables.transform(value, valueExtrator)
        ));
    }
}
