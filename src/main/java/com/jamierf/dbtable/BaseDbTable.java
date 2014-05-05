package com.jamierf.dbtable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import com.jamierf.dbtable.util.container.TableContainerBuilder;
import com.jamierf.dbtable.util.folder.StandardFolder;
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
        this.tableName = Preconditions.checkNotNull(tableName);
        this.handle = Preconditions.checkNotNull(dbi).open();

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
        checkHandle().execute(String.format("CREATE TABLE IF NOT EXISTS %s (row_field VARCHAR NOT NULL, column_field VARCHAR NOT NULL, value_field BLOB NOT NULL, PRIMARY KEY (row_field, column_field))", tableName));
    }

    @Override
    public boolean contains(Object row, Object column) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName))
                .bind("row_field", row)
                .bind("column_field", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsRow(Object row) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE row_field = :row_field", tableName))
                .bind("row_field", row)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsColumn(Object column) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE column_field = :column_field", tableName))
                .bind("column_field", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %s WHERE value_field = :value_field", tableName))
                .bind("value_field", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public byte[] get(Object row, Object column) {
        return checkHandle().createQuery(String.format("SELECT value_field FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName))
                .bind("row_field", row)
                .bind("column_field", column)
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
        return checkHandle().createQuery(String.format("SELECT COUNT(value_field) FROM %s", tableName))
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
        checkHandle().insert(String.format("MERGE INTO %s VALUES (:row_field, :column_field, :value_field)", tableName), row, column, value);
        return result;
    }

    @Override
    public void putAll(@Nullable Table<? extends byte[], ? extends byte[], ? extends byte[]> table) {
        final PreparedBatch batch = checkHandle().prepareBatch(String.format("MERGE INTO %s VALUES (:row_field, :column_field, :value_field)", tableName));

        for (Table.Cell<? extends byte[], ? extends byte[], ? extends byte[]> cell : table.cellSet()) {
            batch.add()
                    .bind("row_field", cell.getRowKey())
                    .bind("column_field", cell.getColumnKey())
                    .bind("value_field", cell.getValue());
        }

        batch.execute();
    }

    @Override
    public byte[] remove(Object row, Object column) {
        final byte[] result = get(row, column);
        checkHandle().execute(String.format("DELETE FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName), row, column);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> row(@Nullable byte[] row) {
        return new DbMap(tableName, "row_field", row, "column_field", handle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> column(@Nullable byte[] column) {
        return new DbMap(tableName, "column_field", column, "row_field", handle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Cell<byte[], byte[], byte[]>> cellSet() {
        return checkHandle().createQuery(String.format("SELECT row_field, column_field, value_field FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row_field", "column_field", "value_field"))
                .list(Set.class); // TODO: Make this dynamic
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> rowKeySet() {
        return new DbSet(tableName, "row_field", handle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> columnKeySet() {
        return new DbSet(tableName, "column_field", handle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<byte[]> values() {
        return checkHandle().createQuery(String.format("SELECT value_field FROM %s", tableName))
                .map(ByteArrayMapper.FIRST)
                .list(List.class); // TODO: Make this dynamic
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> rowMap() {
        return checkHandle().createQuery(String.format("SELECT row_field, column_field, value_field FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row_field", "column_field", "value_field"))
                .fold(TableContainerBuilder.<byte[], byte[], byte[]>getInstance(), TABLE_CELL_FOLDER)
                .build().rowMap(); // TODO: Make this dynamic
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> columnMap() {
        return checkHandle().createQuery(String.format("SELECT row_field, column_field, value_field FROM %s", tableName))
                .map(new ByteArrayTableCellMapper("row_field", "column_field", "value_field"))
                .fold(TableContainerBuilder.<byte[], byte[], byte[]>getInstance(), TABLE_CELL_FOLDER)
                .build().columnMap(); // TODO: Make this dynamic
    }

    public synchronized void delete() {
        if (handle != null) {
            handle.execute(String.format("DROP TABLE %s", tableName));
            close();
        }
    }
}
