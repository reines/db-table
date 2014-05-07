package com.jamierf.dbtable.core.mapper.result.field;

public interface FieldMapperFactory<T> {
    FieldMapper<T> build(String fieldName);
}
