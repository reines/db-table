package com.jamierf.dbtable.core.mapper.result.field;

public class ByteArrayFieldMapperFactory implements FieldMapperFactory<byte[]> {
    @Override
    public FieldMapper<byte[]> build(String fieldName) {
        return new ByteArrayFieldMapper(fieldName);
    }
}
