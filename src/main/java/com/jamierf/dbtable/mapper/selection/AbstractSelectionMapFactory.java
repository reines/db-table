package com.jamierf.dbtable.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.jamierf.dbtable.util.CastFunction;

import java.util.Collection;

public abstract class AbstractSelectionMapFactory<T> {

    private final Function<Object, T> cast = CastFunction.getInstance();

    public abstract Collection<String> fields();

    protected abstract SelectionMap getSelectionMap(T value);

    @SuppressWarnings("unchecked")
    public final SelectionMap get(Object value) {
        return getSelectionMap(cast.apply(value));
    }

    protected abstract SelectionMap getSelectionMap(Iterable<T> value);

    @SuppressWarnings("unchecked")
    public final SelectionMap get(Iterable<?> value) {
        return getSelectionMap(Iterables.transform(value, cast));
    }
}
