package com.jamierf.dbtable.mapper.result.table;

import com.jamierf.dbtable.mapper.result.field.FieldMapperFactory;
import com.jamierf.dbtable.mapper.result.map.MapEntryMapperFactory;

public class TableCellMapperFactory<R, C, V> {

    private final FieldMapperFactory<R> rowMapperFactory;
    private final FieldMapperFactory<C> columnMapperFactory;
    private final FieldMapperFactory<V> valueMapperFactory;

    public TableCellMapperFactory(FieldMapperFactory<R> rowMapperFactory, FieldMapperFactory<C> columnMapperFactory, FieldMapperFactory<V> valueMapperFactory) {
        this.rowMapperFactory = rowMapperFactory;
        this.columnMapperFactory = columnMapperFactory;
        this.valueMapperFactory = valueMapperFactory;
    }

    public TableCellMapper<R, C, V> build(String rowFieldName, String columnFieldName, String valueFieldName) {
        return new TableCellMapper<>(
                rowMapperFactory.build(rowFieldName),
                columnMapperFactory.build(columnFieldName),
                valueMapperFactory.build(valueFieldName)
        );
    }

    public MapEntryMapperFactory<R, V> getRowMapMapperFactory() {
        return new MapEntryMapperFactory<>(rowMapperFactory, valueMapperFactory);
    }

    public MapEntryMapperFactory<C, V> getColumnMapMapperFactory() {
        return new MapEntryMapperFactory<>(columnMapperFactory, valueMapperFactory);
    }
}
