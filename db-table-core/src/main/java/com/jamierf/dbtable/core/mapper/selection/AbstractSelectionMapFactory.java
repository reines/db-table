package com.jamierf.dbtable.core.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.Collection;

public abstract class AbstractSelectionMapFactory<T> {

    private final Function<Object, T> cast = new Function<Object, T>() {
        @Nullable
        @Override
        @SuppressWarnings("unchecked")
        public T apply(@Nullable Object input) {
            return (T) input;
        }
    };

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
