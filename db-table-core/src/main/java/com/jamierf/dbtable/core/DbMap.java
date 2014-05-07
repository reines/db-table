package com.jamierf.dbtable.core;

import com.google.common.base.Joiner;
import com.jamierf.dbtable.core.mapper.result.field.FieldMapper;
import com.jamierf.dbtable.core.mapper.result.map.MapEntryMapper;
import com.jamierf.dbtable.core.mapper.selection.AbstractSelectionMapFactory;
import com.jamierf.dbtable.core.mapper.selection.FieldSelectionMapFactory;
import com.jamierf.dbtable.core.mapper.selection.MapEntrySelectionMapFactory;
import com.jamierf.dbtable.core.mapper.selection.SelectionMap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

class DbMap<K, V> extends AbstractMap<K, V> {

    private final String tableName;
    private final SelectionMap selectionMap;
    private final Handle handle;
    private final AbstractSelectionMapFactory<K> selectionMapFactory;
    private final MapEntryMapper<K, V> mapEntryMapper;

    DbMap(String tableName, Handle handle, SelectionMap selectionMap, AbstractSelectionMapFactory<K> selectionMapFactory, MapEntryMapper<K, V> mapEntryMapper) {
        this.tableName = tableName;
        this.handle = handle;
        this.selectionMap = selectionMap;
        this.selectionMapFactory = selectionMapFactory;
        this.mapEntryMapper = mapEntryMapper;
    }

    @Override
    public int size() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.keyFields());
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT (%2$s)) FROM %1$s WHERE %3$s", tableName, fieldsToCount, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s", tableName, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .first() == null;
    }

    @Override
    public boolean containsKey(Object key) {
        final SelectionMap valueMap = selectionMapFactory.get(key);
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE value_field = :value_field AND %2$s", tableName, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bind("value_field", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public V get(Object key) {
        final SelectionMap valueMap = selectionMapFactory.get(key);
        return handle.createQuery(String.format("SELECT * FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .map(mapEntryMapper.getValueMapper())
                .first();
    }

    @Override
    public V put(K key, V value) {
        final V result = get(key);

        final SelectionMap valueMap = selectionMapFactory.get(key);
        handle.createStatement(String.format("REPLACE INTO %1$s (row_field, column_field, value_field) VALUES (:row_field, :column_field, :value_field)", tableName))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .bind("value_field", value)
                .execute();

        return result;
    }

    @Override
    public V remove(Object key) {
        final V result = get(key);

        final SelectionMap valueMap = selectionMapFactory.get(key);
        handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .execute();

        return result;
    }

    @Override
    public void putAll(@Nullable Map<? extends K, ? extends V> map) {
        final PreparedBatch batch = handle.prepareBatch(String.format("REPLACE INTO %1$s (row_field, column_field, value_field) VALUES (:row_field, :column_field, :value_field)", tableName));

        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            final SelectionMap valueMap = selectionMapFactory.get(entry.getKey());
            batch.add()
                    .bindFromMap(selectionMap.asMap())
                    .bindFromMap(valueMap.asMap())
                    .bind("value_field", entry.getValue());
        }

        batch.execute();
    }

    @Override
    public void clear() {
        handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s", tableName, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .execute();
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        final FieldMapper<K> mapper = mapEntryMapper.getKeyMapper();
        return new DbSet<>(tableName, handle, selectionMap, new FieldSelectionMapFactory<K>(mapper.getFieldName()), mapper);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        final FieldMapper<V> mapper = mapEntryMapper.getValueMapper();
        return new DbCollection<>(tableName, handle, selectionMap, new FieldSelectionMapFactory<V>(mapper.getFieldName()), mapper);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet() {
        return new DbSet<>(tableName, handle, selectionMap, new MapEntrySelectionMapFactory<K, V>(mapEntryMapper.getKeyMapper().getFieldName(), mapEntryMapper.getValueMapper().getFieldName()), mapEntryMapper);
    }
}
