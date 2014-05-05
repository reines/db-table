package com.jamierf.dbtable.util.mapper.result;

import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteArrayTableCellMapper extends TableCellMapper<byte[], byte[], byte[]> {

    private final String rowFieldName;
    private final String columnFieldName;
    private final String valueFieldName;

    public ByteArrayTableCellMapper(String rowFieldName, String columnFieldName, String valueFieldName) {
        this.rowFieldName = rowFieldName;
        this.columnFieldName = columnFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    protected byte[] mapRow(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(rowFieldName);
    }

    @Override
    protected byte[] mapColumn(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(columnFieldName);
    }

    @Override
    protected byte[] mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(valueFieldName);
    }
}
