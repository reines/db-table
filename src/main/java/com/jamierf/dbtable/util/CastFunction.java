package com.jamierf.dbtable.util;

import com.google.common.base.Function;

import javax.annotation.Nullable;

public class CastFunction<T> implements Function<Object, T> {

    public static <T> Function<Object, T> getInstance() {
        return new CastFunction<>();
    }

    private CastFunction() {}

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public T apply(@Nullable Object input) {
        return (T) input;
    }
}
