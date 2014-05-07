package com.jamierf.dbtable.core.mapper.result.field;

import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteArrayFieldMapper extends FieldMapper<byte[]> {

    public ByteArrayFieldMapper(String fieldName) {
        super (fieldName);
    }

    @Override
    public byte[] map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getBytes(getFieldName());
    }
}
