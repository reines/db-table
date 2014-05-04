package com.jamierf.dbtable.util.mapper;

import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteArrayTableCellMapper extends TableCellMapper<byte[], byte[], byte[]> {

    private final String rowKey;
    private final String columnKey;
    private final String valueKey;

    public ByteArrayTableCellMapper(String rowKey, String columnKey, String valueKey) {
        this.rowKey = rowKey;
        this.columnKey = columnKey;
        this.valueKey = valueKey;
    }

    @Override
    protected byte[] mapRow(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(rowKey);
    }

    @Override
    protected byte[] mapColumn(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(columnKey);
    }

    @Override
    protected byte[] mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(valueKey);
    }
}
