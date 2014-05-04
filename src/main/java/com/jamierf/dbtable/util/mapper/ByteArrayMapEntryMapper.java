package com.jamierf.dbtable.util.mapper;

import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteArrayMapEntryMapper extends MapEntryMapper<byte[], byte[]> {

    private final String keyColumn;
    private final String valueColumn;

    public ByteArrayMapEntryMapper(String keyColumn, String valueColumn) {
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
    }

    @Override
    protected byte[] mapKey(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(keyColumn);
    }

    @Override
    protected byte[] mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(valueColumn);
    }
}
