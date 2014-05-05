package com.jamierf.dbtable.util.mapper.selection;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.jamierf.dbtable.util.mapper.TableCellExtractor;
import com.jamierf.dbtable.util.mapper.result.ByteArrayTableCellMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.util.Collection;

public class ByteArrayCellSelectionMapFactory extends AbstractSelectionMapFactory<Table.Cell<byte[], byte[], byte[]>> {

    private static final Function<Table.Cell<byte[], byte[], byte[]>, byte[]> ROW_EXTRACTOR = TableCellExtractor.getRow();
    private static final Function<Table.Cell<byte[], byte[], byte[]>, byte[]> COLUMN_EXTRACTOR = TableCellExtractor.getColumn();
    private static final Function<Table.Cell<byte[], byte[], byte[]>, byte[]> VALUE_EXTRACTOR = TableCellExtractor.getValue();

    private final String rowFieldName;
    private final String columnFieldName;
    private final String valueFieldName;

    public ByteArrayCellSelectionMapFactory(String rowFieldName, String columnFieldName, String valueFieldName) {
        this.rowFieldName = rowFieldName;
        this.columnFieldName = columnFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    public Collection<String> fields() {
        return ImmutableSet.of(rowFieldName, columnFieldName, valueFieldName);
    }

    @Override
    public SelectionMap getSelectionMap(Table.Cell<byte[], byte[], byte[]> value) {
        return SelectionMap.of(
                rowFieldName, value.getRowKey(),
                columnFieldName, value.getColumnKey(),
                valueFieldName, value.getValue()
        );
    }

    @Override
    public SelectionMap getSelectionMap(Iterable<Table.Cell<byte[], byte[], byte[]>> value) {
        return SelectionMap.of(
                rowFieldName, Iterables.transform(value, ROW_EXTRACTOR),
                columnFieldName, Iterables.transform(value, COLUMN_EXTRACTOR),
                valueFieldName, Iterables.transform(value, VALUE_EXTRACTOR)
        );
    }

    @Override
    public ResultSetMapper<Table.Cell<byte[], byte[], byte[]>> mapper() {
        return new ByteArrayTableCellMapper(rowFieldName, columnFieldName, valueFieldName);
    }
}
