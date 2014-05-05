package com.jamierf.dbtable.util.mapper.selection;

import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.TypedMapper;

import java.util.Collection;

public class ByteArraySelectionMapFactory extends AbstractSelectionMapFactory<byte[]> {

    private final String fieldName;

    public ByteArraySelectionMapFactory(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Collection<String> fields() {
        return ImmutableSet.of(fieldName);
    }

    @Override
    public SelectionMap getSelectionMap(byte[] value) {
        return SelectionMap.of(fieldName, value);
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<byte[]> value) {
        return SelectionMap.of(fieldName, value);
    }

    @Override
    public TypedMapper<byte[]> mapper() {
        return ByteArrayMapper.FIRST;
    }
}
