package com.jamierf.dbtable.util.mapper;

import com.google.common.base.Function;
import com.google.common.collect.Table;

import javax.annotation.Nullable;

public final class TableCellExtractor {

    public static <R, C, V> Function<Table.Cell<R, C, V>, R> getRow() {
        return new Function<Table.Cell<R, C, V>, R>() {
            @Nullable
            @Override
            public R apply(@Nullable Table.Cell<R, C, V> input) {
                return input == null ? null : input.getRowKey();
            }
        };
    }

    public static <R, C, V> Function<Table.Cell<R, C, V>, C> getColumn() {
        return new Function<Table.Cell<R, C, V>, C>() {
            @Nullable
            @Override
            public C apply(@Nullable Table.Cell<R, C, V> input) {
                return input == null ? null : input.getColumnKey();
            }
        };
    }

    public static <R, C, V> Function<Table.Cell<R, C, V>, V> getValue() {
        return new Function<Table.Cell<R, C, V>, V>() {
            @Nullable
            @Override
            public V apply(@Nullable Table.Cell<R, C, V> input) {
                return input == null ? null : input.getValue();
            }
        };
    }

    private TableCellExtractor() {}
}
