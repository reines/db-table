package com.jamierf.dbtable.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Table;
import com.jamierf.dbtable.core.DbTable;
import com.jamierf.dbtable.jackson.codec.ObjectMapperCodec;
import com.yammer.collections.transforming.TransformingTable;
import org.skife.jdbi.v2.Handle;

import static com.google.common.base.Preconditions.checkNotNull;

public final class JacksonDbTableBuilder {

    private final Handle handle;

    private ObjectMapper mapper = new ObjectMapper();

    public JacksonDbTableBuilder(Handle handle) {
        this.handle = checkNotNull(handle);
    }

    public JacksonDbTableBuilder using(ObjectMapper mapper) {
        this.mapper = checkNotNull(mapper);
        return this;
    }

    public JacksonDbTableBuilder using(JsonFactory jsonFactory) {
        return using(new ObjectMapper(jsonFactory));
    }

    public <R, C, V> Table<R, C, V> build(String tableName, Class<R> rowType, Class<C> columnType, Class<V> valueType) {
        final ObjectMapperCodec codec = new ObjectMapperCodec(mapper);
        return TransformingTable.create(new DbTable(tableName, handle),
                codec.<R>newEncoder(), codec.newDecoder(rowType),
                codec.<C>newEncoder(), codec.newDecoder(columnType),
                codec.<V>newEncoder(), codec.newDecoder(valueType)
        );
    }
}
