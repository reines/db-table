package com.jamierf.dbtable.util.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.jamierf.dbtable.util.mapper.MapEntryExtractor;
import com.jamierf.dbtable.util.mapper.result.ByteArrayMapEntryMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.util.Collection;
import java.util.Map;

public class ByteArrayEntrySelectionMapFactory extends AbstractSelectionMapFactory<Map.Entry<byte[], byte[]>> {

    private static final Function<Map.Entry<byte[], byte[]>, byte[]> KEY_EXTRACTOR = MapEntryExtractor.getKey();
    private static final Function<Map.Entry<byte[], byte[]>, byte[]> VALUE_EXTRACTOR = MapEntryExtractor.getValue();

    private final String keyFieldName;
    private final String valueFieldName;

    public ByteArrayEntrySelectionMapFactory(String keyFieldName, String valueFieldName) {
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    public Collection<String> fields() {
        return ImmutableSet.of(keyFieldName, valueFieldName);
    }

    @Override
    public SelectionMap getSelectionMap(Map.Entry<byte[], byte[]> value) {
        return SelectionMap.of(
                keyFieldName, value.getKey(),
                valueFieldName, value.getValue()
        );
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<Map.Entry<byte[], byte[]>> value) {
        return SelectionMap.of(
                keyFieldName, Iterables.transform(value, KEY_EXTRACTOR),
                valueFieldName, Iterables.transform(value, VALUE_EXTRACTOR)
        );
    }

    @Override
    public ResultSetMapper<Map.Entry<byte[], byte[]>> mapper() {
        return new ByteArrayMapEntryMapper(keyFieldName, valueFieldName);
    }
}
