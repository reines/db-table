package com.jamierf.dbtable.util.mapper.result;

import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteArrayMapEntryMapper extends MapEntryMapper<byte[], byte[]> {

    private final String keyFieldName;
    private final String valueFieldName;

    public ByteArrayMapEntryMapper(String keyFieldName, String valueFieldName) {
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    protected byte[] mapKey(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(keyFieldName);
    }

    @Override
    protected byte[] mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(valueFieldName);
    }
}
