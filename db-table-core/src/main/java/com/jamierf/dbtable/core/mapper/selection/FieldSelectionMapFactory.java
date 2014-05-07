package com.jamierf.dbtable.core.mapper.selection;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;

public class FieldSelectionMapFactory<T> extends AbstractSelectionMapFactory<T> {

    private final String fieldName;

    public FieldSelectionMapFactory(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Collection<String> keyFields() {
        return ImmutableSet.of(fieldName);
    }

    @Override
    public SelectionMap getSelectionMap(T value) {
        return new SelectionMap(ImmutableMap.<String, Object>of(
                fieldName, value
        ));
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<T> value) {
        return new SelectionMap(ImmutableMap.<String, Object>of(
                fieldName, value
        ));
    }
}
