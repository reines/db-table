package com.jamierf.dbtable.mapper.result.field;

public interface FieldMapperFactory<T> {
    FieldMapper<T> build(String fieldName);
}
