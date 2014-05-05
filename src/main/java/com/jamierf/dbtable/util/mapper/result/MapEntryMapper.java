package com.jamierf.dbtable.util.mapper.result;

import com.google.common.collect.Maps;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public abstract class MapEntryMapper<K, V> implements ResultSetMapper<Map.Entry<K, V>> {

    @Override
    public Map.Entry<K, V> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return Maps.immutableEntry(
                mapKey(index, r, ctx),
                mapValue(index, r, ctx)
        );
    }

    protected abstract K mapKey(int index, ResultSet r, StatementContext ctx) throws SQLException;
    protected abstract V mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException;
}
