package com.jamierf.dbtable;

import com.jamierf.dbtable.util.mapper.selection.SelectionMap;
import com.jamierf.dbtable.util.mapper.selection.ByteArrayEntrySelectionMapFactory;
import com.jamierf.dbtable.util.mapper.selection.ByteArraySelectionMapFactory;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

// TODO: Make generic
class DbMap extends AbstractMap<byte[], byte[]> {

    private final String tableName;
    private final SelectionMap selectionMap;
    private final String keyField;
    private final Handle handle;

    DbMap(String tableName, SelectionMap selectionMap, String keyField, Handle handle) {
        this.tableName = tableName;
        this.selectionMap = selectionMap;
        this.keyField = keyField;
        this.handle = handle;
    }

    @Override
    public int size() {
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s WHERE " + selectionMap.asSql(), tableName, keyField))
                .bindFromMap(selectionMap.asMap())
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE " + selectionMap.asSql(), tableName))
                .bindFromMap(selectionMap.asMap())
                .first() == null;
    }

    @Override
    public boolean containsKey(Object key) {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s AND " + selectionMap.asSql(), tableName, keyField))
                .bindFromMap(selectionMap.asMap())
                .bind(keyField, key)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE value_field = :value_field AND " + selectionMap.asSql(), tableName))
                .bindFromMap(selectionMap.asMap())
                .bind("value_field", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public byte[] get(Object key) {
        return handle.createQuery(String.format("SELECT value_field FROM %1$s WHERE %2$s = :%2$s AND " + selectionMap.asSql(), tableName, keyField))
                .bindFromMap(selectionMap.asMap())
                .bind(keyField, key)
                .map(ByteArrayMapper.FIRST)
                .first();
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        final byte[] result = get(key);
        handle.createStatement(String.format("MERGE INTO %1$s (row_field, column_field, value_field) VALUES (:row_field, :column_field, :value_field)", tableName))
                .bindFromMap(selectionMap.asMap())
                .bind(keyField, key)
                .bind("value_field", value)
                .execute();
        return result;
    }

    @Override
    public byte[] remove(Object key) {
        final byte[] result = get(key);
        handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s = :%2$s AND " + selectionMap.asSql(), tableName, keyField))
                .bindFromMap(selectionMap.asMap())
                .bind(keyField, key)
                .execute();
        return result;
    }

    @Override
    public void putAll(@Nullable Map<? extends byte[], ? extends byte[]> map) {
        final PreparedBatch batch = handle.prepareBatch(String.format("MERGE INTO %1$s (row_field, column_field, value_field) VALUES (:row_field, :column_field, :value_field)", tableName));

        for (Map.Entry<? extends byte[], ? extends byte[]> entry : map.entrySet()) {
            batch.add()
                    .bindFromMap(selectionMap.asMap())
                    .bind(keyField, entry.getKey())
                    .bind("value_field", entry.getValue());
        }

        batch.execute();
    }

    @Override
    public void clear() {
        handle.createStatement(String.format("DELETE FROM %1$s WHERE " + selectionMap.asSql(), tableName))
                .bindFromMap(selectionMap.asMap())
                .execute();
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<byte[]> keySet() {
        return new DbSet(tableName, handle, selectionMap, new ByteArraySelectionMapFactory(keyField));
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Collection<byte[]> values() {
        return new DbCollection<>(tableName, handle, selectionMap, new ByteArraySelectionMapFactory("value_field"));
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<Entry<byte[], byte[]>> entrySet() {
        return new DbSet<>(tableName, handle, selectionMap, new ByteArrayEntrySelectionMapFactory(keyField, "value_field"));
    }
}
