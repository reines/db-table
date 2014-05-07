package com.jamierf.dbtable.core.mapper.result.table;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.jamierf.dbtable.core.mapper.result.field.FieldMapper;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableCellMapper<R, C, V> implements ResultSetMapper<Table.Cell<R, C, V>> {

    private final FieldMapper<R> rowMapper;
    private final FieldMapper<C> columnMapper;
    private final FieldMapper<V> valueMapper;

    public TableCellMapper(FieldMapper<R> rowMapper, FieldMapper<C> columnMapper, FieldMapper<V> valueMapper) {
        this.rowMapper = rowMapper;
        this.columnMapper = columnMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    public Table.Cell<R, C, V> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return Tables.immutableCell(
                rowMapper.map(index, r, ctx),
                columnMapper.map(index, r, ctx),
                valueMapper.map(index, r, ctx)
        );
    }

    public FieldMapper<R> getRowMapper() {
        return rowMapper;
    }

    public FieldMapper<C> getColumnMapper() {
        return columnMapper;
    }

    public FieldMapper<V> getValueMapper() {
        return valueMapper;
    }
}
