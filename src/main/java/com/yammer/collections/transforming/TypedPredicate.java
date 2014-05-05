package com.yammer.collections.transforming;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;

public class TypedPredicate<T> implements Predicate<Object> {

    public static <T> Predicate<Object> wrap(Predicate<T> delegate) {
        return new TypedPredicate<>(delegate);
    }

    private final Predicate<T> delegate;

    private TypedPredicate(Predicate<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(@Nullable Object input) {
        return delegate.apply((T) input);
    }
}
