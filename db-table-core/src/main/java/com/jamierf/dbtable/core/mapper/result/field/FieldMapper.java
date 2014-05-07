package com.jamierf.dbtable.core.mapper.result.field;

import org.skife.jdbi.v2.tweak.ResultSetMapper;

public abstract class FieldMapper<T> implements ResultSetMapper<T> {

    private final String fieldName;

    protected FieldMapper(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }
}
