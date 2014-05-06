package com.jamierf.dbtable.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.jamierf.dbtable.util.CastFunction;

import java.util.Collection;

public abstract class AbstractSelectionMapFactory<T> {

    private final Function<Object, T> cast = CastFunction.getInstance();

    protected abstract SelectionMap getSelectionMap(T value);
    protected abstract SelectionMap getSelectionMap(Iterable<T> value);

    public abstract Collection<String> keyFields();

    @SuppressWarnings("unchecked")
    public final SelectionMap get(Object key) {
        return getSelectionMap(cast.apply(key));
    }

    @SuppressWarnings("unchecked")
    public final SelectionMap get(Iterable<?> keys) {
        return getSelectionMap(Iterables.transform(keys, cast));
    }
}
