package com.jamierf.dbtable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import com.jamierf.dbtable.mapper.result.field.ByteArrayFieldMapperFactory;
import com.jamierf.dbtable.mapper.result.field.FieldMapper;
import com.jamierf.dbtable.mapper.result.map.MapEntryMapper;
import com.jamierf.dbtable.mapper.result.table.TableCellMapper;
import com.jamierf.dbtable.mapper.result.table.TableCellMapperFactory;
import com.jamierf.dbtable.mapper.selection.FieldSelectionMapFactory;
import com.jamierf.dbtable.mapper.selection.SelectionMap;
import com.jamierf.dbtable.mapper.selection.TableCellSelectionMapFactory;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DbTable implements Table<byte[], byte[], byte[]> {

    private static final TableCellMapperFactory<byte[], byte[], byte[]> TABLE_CELL_MAPPER_FACTORY = new TableCellMapperFactory<>(
            new ByteArrayFieldMapperFactory(), new ByteArrayFieldMapperFactory(), new ByteArrayFieldMapperFactory()
    );

    private final String tableName;
    private final Handle handle;
    private final TableCellMapper<byte[], byte[], byte[]> tableCellMapper;

    public DbTable(String tableName, Handle handle) {
        this.tableName = Preconditions.checkNotNull(tableName);
        this.handle = Preconditions.checkNotNull(handle);

        tableCellMapper = TABLE_CELL_MAPPER_FACTORY.build("row_field", "column_field", "value_field");

        createTableIfRequired();
    }

    private void createTableIfRequired() {
        handle.execute(String.format("CREATE TABLE IF NOT EXISTS %s (row_field VARBINARY(255) NOT NULL, column_field VARBINARY(255) NOT NULL, value_field BLOB NOT NULL, PRIMARY KEY HASH (row_field, column_field))", tableName));
    }

    @Override
    public boolean contains(Object row, Object column) {
        return handle.createQuery(String.format("SELECT 1 FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName))
                .bind("row_field", row)
                .bind("column_field", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsRow(Object row) {
        return handle.createQuery(String.format("SELECT 1 FROM %s WHERE row_field = :row_field", tableName))
                .bind("row_field", row)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsColumn(Object column) {
        return handle.createQuery(String.format("SELECT 1 FROM %s WHERE column_field = :column_field", tableName))
                .bind("column_field", column)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return handle.createQuery(String.format("SELECT 1 FROM %s WHERE value_field = :value_field", tableName))
                .bind("value_field", value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public byte[] get(Object row, Object column) {
        return handle.createQuery(String.format("SELECT value_field FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName))
                .bind("row_field", row)
                .bind("column_field", column)
                .map(ByteArrayMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return handle.createQuery(String.format("SELECT 1 FROM %s", tableName))
                .map(IntegerMapper.FIRST)
                .first() == null;
    }

    @Override
    public int size() {
        return handle.createQuery(String.format("SELECT COUNT(value_field) FROM %s", tableName))
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public void clear() {
        handle.execute(String.format("TRUNCATE TABLE %s", tableName));
    }

    @Override
    public byte[] put(@Nullable byte[] row, @Nullable byte[] column, @Nullable byte[] value) {
        final byte[] result = get(row, column);
        handle.insert(String.format("REPLACE INTO %s VALUES (:row_field, :column_field, :value_field)", tableName), row, column, value);
        return result;
    }

    @Override
    public void putAll(@Nullable Table<? extends byte[], ? extends byte[], ? extends byte[]> table) {
        final PreparedBatch batch = handle.prepareBatch(String.format("REPLACE INTO %s VALUES (:row_field, :column_field, :value_field)", tableName));

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
        handle.execute(String.format("DELETE FROM %s WHERE row_field = :row_field AND column_field = :column_field", tableName), row, column);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> row(@Nullable byte[] row) {
        final MapEntryMapper<byte[], byte[]> mapper = TABLE_CELL_MAPPER_FACTORY.getRowMapMapperFactory().build("column_field", "value_field");
        return new DbMap(tableName, handle, SelectionMap.of("row_field", row), new FieldSelectionMapFactory("column_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], byte[]> column(@Nullable byte[] column) {
        final MapEntryMapper<byte[], byte[]> mapper = TABLE_CELL_MAPPER_FACTORY.getColumnMapMapperFactory().build("row_field", "value_field");
        return new DbMap<>(tableName, handle, SelectionMap.of("column_field", column), new FieldSelectionMapFactory<byte[]>("row_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Cell<byte[], byte[], byte[]>> cellSet() {
        return new DbSet<>(tableName, handle, SelectionMap.NONE, new TableCellSelectionMapFactory<byte[], byte[], byte[]>(
                "row_field", "column_field", "value_field"), tableCellMapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> rowKeySet() {
        final FieldMapper<byte[]> mapper = tableCellMapper.getRowMapper();
        return new DbSet<>(tableName, handle, SelectionMap.NONE, new FieldSelectionMapFactory<byte[]>("row_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<byte[]> columnKeySet() {
        final FieldMapper<byte[]> mapper = tableCellMapper.getColumnMapper();
        return new DbSet<>(tableName, handle, SelectionMap.NONE, new FieldSelectionMapFactory<byte[]>("column_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<byte[]> values() {
        final FieldMapper<byte[]> mapper = tableCellMapper.getValueMapper();
        return new DbCollection<>(tableName, handle, SelectionMap.NONE, new FieldSelectionMapFactory<byte[]>("value_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> rowMap() {
        final MapEntryMapper<byte[], Map<byte[], byte[]>> mapper = new MapEntryMapper<>(tableCellMapper.getRowMapper(), new FieldMapper<Map<byte[], byte[]>>("column_field, value_field") {
            @Override
            public Map<byte[], byte[]> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
                return row(tableCellMapper.getRowMapper().map(index, r, ctx));
            }
        });
        return new DbMap<>(tableName, handle, SelectionMap.NONE, new FieldSelectionMapFactory<byte[]>("row_field"), mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<byte[], Map<byte[], byte[]>> columnMap() {
        final MapEntryMapper<byte[], Map<byte[], byte[]>> mapper = new MapEntryMapper<>(tableCellMapper.getColumnMapper(), new FieldMapper<Map<byte[], byte[]>>("row_field, value_field") {
            @Override
            public Map<byte[], byte[]> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
                return column(tableCellMapper.getColumnMapper().map(index, r, ctx));
            }
        });
        return new DbMap<>(tableName, handle, SelectionMap.NONE, new FieldSelectionMapFactory<byte[]>("column_field"), mapper);
    }
}
