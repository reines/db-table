package com.jamierf.dbtable.core.mapper.result.map;

import com.google.common.collect.Maps;
import com.jamierf.dbtable.core.mapper.result.field.FieldMapper;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class MapEntryMapper<K, V> implements ResultSetMapper<Map.Entry<K, V>> {

    private final FieldMapper<K> keyMapper;
    private final FieldMapper<V> valueMapper;

    public MapEntryMapper(FieldMapper<K> keyMapper, FieldMapper<V> valueMapper) {
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    public Map.Entry<K, V> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return Maps.immutableEntry(
                keyMapper.map(index, r, ctx),
                valueMapper.map(index, r, ctx)
        );
    }

    public FieldMapper<K> getKeyMapper() {
        return keyMapper;
    }

    public FieldMapper<V> getValueMapper() {
        return valueMapper;
    }
}
