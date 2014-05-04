package com.jamierf.dbtable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import com.jamierf.dbtable.util.container.MapContainerBuilder;
import com.jamierf.dbtable.util.container.TableContainerBuilder;
import com.jamierf.dbtable.util.folder.StandardFolder;
import com.jamierf.dbtable.util.mapper.ByteArrayMapEntryMapper;
import com.jamierf.dbtable.util.mapper.ByteArrayTableCellMapper;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseDbTable implements Table<byte[], byte[], byte[]>, AutoCloseable {

    private static final Folder3<ContainerBuilder<Map<byte[], byte[]>>, Map.Entry<byte[], byte[]>> MAP_ENTRY_FOLDER = StandardFolder.getInstance();
    private static final Folder3<ContainerBuilder<Table<byte[], byte[], byte[]>>, Table.Cell<byte[], byte[], byte[]>> TABLE_CELL_FOLDER = StandardFolder.getInstance();

    private final String tableName;

    private Handle handle;

    public BaseDbTable(String tableName, DBI dbi) {
        this.tableName = tableName;

        handle = dbi.open();

        createTableIfRequired();
    }

    @Override
    public synchronized void close() {
        if (handle != null) {
            handle.close();
            handle = null;
        }
    }

    private Handle checkHandle() {
        Preconditions.checkState(handle != null, "This table has been closed.");
        return handle;
    }

    private void createTableIfRequired() {
        checkHandle().execute(String.format("CREATE TABLE IF NOT EXISTS %s (row VARCHAR NOT NULL, column VARCHAR NOT NULL, value BLOB NOT NULL, PRIMARY KEY (row, column))", tableName));
    }

    @Override
    public boolean contains(Object row, Object column) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE row = :row AND column = :column", tableName))
                .bind("row", row)
                .bind("column", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsRow(Object row) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE row = :row", tableName))
                .bind("row", row)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsColumn(Object column) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE column = :column", tableName))
                .bind("column", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE value = :value", tableName))
                .bind("value", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public byte[] get(Object row, Object column) {
        return checkHandle().createQuery(String.format("SELECT value FROM %s WHERE row = :row AND column = :column", tableName))
                .bind("row", row)
                .bind("column", column)
                .map(ByteArrayMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s", tableName))
                .map(IntegerMapper.FIRST)
                .first() == null;
    }

    @Override
    public int size() {
        return checkHandle().createQuery(String.format("SELECT COUNT(value) FROM %s", tableName))
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public void clear() {
        checkHandle().execute(String.format("TRUNCATE TABLE %s", tableName));
    }

    @Override
    public byte[] put(@Nullable byte[] row, @Nullable byte[] column, @Nullable byte[] value) {
        final byte[] result = get(row, column);
        checkHandle().insert(String.format("MERGE INTO %s VALUES (:row, :column, :value)", tableName), row, column, value);
        return result;
    }

    @Override
    public void putAll(@Nullable Table<? extends byte[], ? extends byte[], ? extends byte[]> table) {
        final PreparedBatch batch = checkHandle().prepareBatch(String.format("MERGE INTO %s VALUES (:row, :column, :value)", tableName));

        for (Table.Cell<? extends byte[], ? extends byte[], ? extends byte[]> cell : table.cellSet()) {
            batch.add()
                    .bind("row", cell.getRowKey())
                    .bind("column", cell.getColumnKey())
                    .bind("value", cell.getValue());
        }

        batch.execute();
    }

    @Override
    public byte[] remove(Object row, Object column) {
        final byte[] result = get(row, column);
        checkHandle().execute(String.format("DELETE FROM %s WHERE row = :row AND column = :column", tableName), row, column);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> row(@Nullable byte[] row) {
        return checkHandle().createQuery(String.format("SELECT column, value FROM %s WHERE row = :row", tableName))
                .bind("row", row)
                .map(new ByteArrayMapEntryMapper("column", "value"))
                .fold(MapContainerBuilder.<byte[], byte[]>getInstance(), MAP_ENTRY_FOLDER)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> column(@Nullable byte[] column) {
        return checkHandle().createQuery(String.format("SELECT row, value FROM %s WHERE column = :column", tableName))
                .bind("column", column)
                .map(new ByteArrayMapEntryMapper("row", "value"))
                .fold(MapContainerBuilder.<byte[], byte[]>getInstance(), MAP_ENTRY_FOLDER)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Cell<byte[], byte[], byte[]>> cellSet() {
        return checkHandle().createQuery(String.format("SELECT row, column, value FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row", "column", "value"))
                .list(Set.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> rowKeySet() {
        return checkHandle().createQuery(String.format("SELECT row FROM %s", tableName))
                .map(ByteArrayMapper.FIRST)
                .list(Set.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> columnKeySet() {
        return checkHandle().createQuery(String.format("SELECT column FROM %s", tableName))
                .map(ByteArrayMapper.FIRST)
                .list(Set.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<byte[]> values() {
        return checkHandle().createQuery(String.format("SELECT value FROM %s", tableName))
                .map(ByteArrayMapper.FIRST)
                .list(List.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> rowMap() {
        return checkHandle().createQuery(String.format("SELECT row, column, value FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row", "column", "value"))
                .fold(TableContainerBuilder.<byte[], byte[], byte[]>getInstance(), TABLE_CELL_FOLDER)
                .build().rowMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> columnMap() {
        return checkHandle().createQuery(String.format("SELECT row, column, value FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row", "column", "value"))
                .fold(TableContainerBuilder.<byte[], byte[], byte[]>getInstance(), TABLE_CELL_FOLDER)
                .build().columnMap();
    }

    public synchronized void delete() {
        if (handle != null) {
            handle.execute(String.format("DROP TABLE %s", tableName));
            close();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("tableName", tableName)
                .add("handle", handle)
                .toString();
    }
}
