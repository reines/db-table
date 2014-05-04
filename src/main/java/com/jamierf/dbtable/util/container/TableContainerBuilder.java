package com.jamierf.dbtable.util.container;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import org.skife.jdbi.v2.ContainerBuilder;

public class TableContainerBuilder<R, C, V> implements ContainerBuilder<Table<R, C, V>> {

    public static <R, C, V> ContainerBuilder<Table<R, C, V>> getInstance() {
        return new TableContainerBuilder<>();
    }

    private final ImmutableTable.Builder<R, C, V> table;

    private TableContainerBuilder() {
        table = ImmutableTable.builder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ContainerBuilder<Table<R, C, V>> add(Object it) {
        if (!(it instanceof Table.Cell)) {
            throw new IllegalArgumentException("MapContainerFactory can only accept Table.Cell values");
        }

        table.put((Table.Cell<R, C, V>) it);
        return this;
    }

    @Override
    public Table<R, C, V> build() {
        return table.build();
    }
}