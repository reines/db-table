package com.jamierf.dbtable;

import com.google.common.base.Preconditions;
import com.jamierf.dbtable.util.mapper.ByteArrayMapEntryMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

class DbMap extends AbstractMap<byte[], byte[]> {

    private final String tableName;
    private final String selectionField;
    private final byte[] selectionValue;
    private final String keyField;
    private final Handle handle;

    DbMap(String tableName, String selectionField, byte[] selectionValue, String keyField, Handle handle) {
        this.tableName = tableName;
        this.selectionField = selectionField;
        this.selectionValue = selectionValue;
        this.keyField = keyField;
        this.handle = handle;
    }

    private Handle checkHandle() {
        Preconditions.checkState(handle != null, "This map has been closed.");
        return handle;
    }

    @Override
    public int size() {
        return checkHandle().createQuery(String.format("SELECT COUNT(DISTINCT %3$s) FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField, keyField))
                .bind(selectionField, selectionValue)
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField))
                .bind(selectionField, selectionValue)
                .first() == null;
    }

    @Override
    public boolean containsKey(Object key) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s AND %3$s = :%3$s", tableName, selectionField, keyField))
                .bind(selectionField, selectionValue)
                .bind(keyField, key)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s AND value_field = :value_field", tableName, selectionField))
                .bind(selectionField, selectionValue)
                .bind("value_field", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public byte[] get(Object key) {
        return checkHandle().createQuery(String.format("SELECT value_field FROM %1$s WHERE %2$s = :%2$s AND %3$s = :%3$s", tableName, selectionField, keyField))
                .bind(selectionField, selectionValue)
                .bind(keyField, key)
                .map(ByteArrayMapper.FIRST)
                .first();
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        final byte[] result = get(key);
        checkHandle().insert(String.format("MERGE INTO %1$s (%2$s, %3$s, value_field) VALUES (:%2$s, :%3$s, :value_field)", tableName, selectionField, keyField), selectionValue, key, value);
        return result;
    }

    @Override
    public byte[] remove(Object key) {
        final byte[] result = get(key);
        checkHandle().execute(String.format("DELETE FROM %1$s WHERE %2$s = :%2$s AND %3$s = :%3$s", tableName, selectionField, keyField), selectionValue, key);
        return result;
    }

    @Override
    public void putAll(@Nullable Map<? extends byte[], ? extends byte[]> map) {
        final PreparedBatch batch = checkHandle().prepareBatch(String.format("MERGE INTO %1$s (%2$s, %3$s, value_field) VALUES (:%2$s, :%3$s, :value_field)", tableName, selectionField, keyField));

        for (Map.Entry<? extends byte[], ? extends byte[]> entry : map.entrySet()) {
            batch.add()
                    .bind(selectionField, selectionValue)
                    .bind(keyField, entry.getKey())
                    .bind("value_field", entry.getValue());
        }

        batch.execute();
    }

    @Override
    public void clear() {
        checkHandle().execute(String.format("DELETE FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField), selectionValue);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<byte[]> keySet() {
        return checkHandle().createQuery(String.format("SELECT %3$s FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField, keyField))
                .bind(selectionField, selectionValue)
                .map(ByteArrayMapper.FIRST)
                .list(Set.class); // TODO: Make this dynamic
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Collection<byte[]> values() {
        return checkHandle().createQuery(String.format("SELECT value_field FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField))
                .bind(selectionField, selectionValue)
                .map(ByteArrayMapper.FIRST)
                .list(List.class); // TODO: Make this dynamic
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Set<Entry<byte[], byte[]>> entrySet() {
        return checkHandle().createQuery(String.format("SELECT %3$s, value_field FROM %1$s WHERE %2$s = :%2$s", tableName, selectionField, keyField))
                .bind(selectionField, selectionValue)
                .map(new ByteArrayMapEntryMapper(keyField, "value_field"))
                .list(Set.class); // TODO: Make this dynamic
    }
}
