package com.jamierf.dbtable.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.jamierf.dbtable.mapper.TableCellExtractor;

import java.util.Collection;

public class TableCellSelectionMapFactory<R, C, V> extends AbstractSelectionMapFactory<Table.Cell<R, C, V>> {

    private final String rowFieldName;
    private final String columnFieldName;
    private final String valueFieldName;

    private final Function<Table.Cell<R, C, V>, R> rowExtractor;
    private final Function<Table.Cell<R, C, V>, C> columnExtractor;
    private final Function<Table.Cell<R, C, V>, V> valueExtractor;

    public TableCellSelectionMapFactory(String rowFieldName, String columnFieldName, String valueFieldName) {
        this.rowFieldName = rowFieldName;
        this.columnFieldName = columnFieldName;
        this.valueFieldName = valueFieldName;

        rowExtractor = TableCellExtractor.getRow();
        columnExtractor = TableCellExtractor.getColumn();
        valueExtractor = TableCellExtractor.getValue();
    }

    @Override
    public Collection<String> keyFields() {
        return ImmutableSet.of(rowFieldName, columnFieldName, valueFieldName);
    }

    @Override
    public SelectionMap getSelectionMap(Table.Cell<R, C, V> value) {
        return new SelectionMap(ImmutableMap.of(
                rowFieldName, value.getRowKey(),
                columnFieldName, value.getColumnKey(),
                valueFieldName, value.getValue()
        ));
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<Table.Cell<R, C, V>> value) {
        return new SelectionMap(ImmutableMap.<String, Object>of(
                rowFieldName, Iterables.transform(value, rowExtractor),
                columnFieldName, Iterables.transform(value, columnExtractor),
                valueFieldName, Iterables.transform(value, valueExtractor)
        ));
    }
}
