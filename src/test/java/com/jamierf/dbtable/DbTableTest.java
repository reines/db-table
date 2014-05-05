package com.jamierf.dbtable;

import com.google.common.collect.*;
import com.jamierf.dbtable.util.codec.StringCodec;
import com.yammer.collections.transforming.TransformingTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DbTableTest {

    private static final String DATABASE_NAME = "test";
    private static final String TEST_ROW = "row";
    private static final String TEST_COLUMN = "column";
    private static final String TEST_VALUE = "value";

    private static final Random RANDOM = new Random();
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private static String randomString(int length) {
        return new BigInteger(length, RANDOM).toString();
    }

    private static Table<String, String, String> createTable(String name, Handle handle) {
        return TransformingTable.create(
                DbTable.create(name, handle),
                StringCodec.ENCODER, StringCodec.DECODER,
                StringCodec.ENCODER, StringCodec.DECODER,
                StringCodec.ENCODER, StringCodec.DECODER
        );
    }

    private Handle handle;
    private Table<String, String, String> table;

    @Before
    public void setUp() {
        handle = DBI.open(String.format("jdbc:h2:mem:test-%s", COUNTER.getAndIncrement()));
        table = createTable(DATABASE_NAME, handle);
    }

    @After
    public void tearDown() {
        handle.close();
    }

    // Test constructor

    @Test
    public void testCreate_CreatesTableWithoutException() {
        assertNotNull(table);
    }

    // Test isEmpty

    @Test
    public void testEmpty_EmptyTableIsEmpty() {
        assertTrue(table.isEmpty());
    }

    @Test
    public void testEmpty_NotEmptyTableIsntEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.isEmpty());
    }

    // Test size

    @Test
    public void testSize_EmptyTableIsZero() {
        assertEquals(0, table.size());
    }

    @Test
    public void testSize_OneRowIsOne() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.size());
    }

    // Test contains

    @Test
    public void testContains_EmptyTableIsEmpty() {
        // Shouldn't contain anything
        assertFalse(table.contains(TEST_ROW, TEST_COLUMN));
        assertFalse(table.containsRow(TEST_ROW));
        assertFalse(table.containsColumn(TEST_COLUMN));
        assertFalse(table.containsValue(TEST_VALUE));
    }

    @Test
    public void testContains_ContainsOnlyExpectedRow() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        // Should contain the row we added
        assertTrue(table.contains(TEST_ROW, TEST_COLUMN));
        assertTrue(table.containsRow(TEST_ROW));
        assertTrue(table.containsColumn(TEST_COLUMN));
        assertTrue(table.containsValue(TEST_VALUE));

        // Shouldn't contain other rows
        assertFalse(table.contains("bla", "bla"));
        assertFalse(table.containsRow("bla"));
        assertFalse(table.containsColumn("bla"));
        assertFalse(table.containsValue("bla"));
    }

    // Test clear

    @Test
    public void testClear_ClearedTableIsEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.clear();

        assertTrue(table.isEmpty());
    }

    // Test put

    @Test
    public void testPut_NonExistingValueDoesntOverwrite() {
        assertNull(table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE));
    }

    @Test
    public void testPut_ExistingValueOverwritten() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertEquals(1, table.size());

        // Reinsert same row, it should overwrite existing
        assertNotNull(table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE));
        assertEquals(1, table.size());
    }

    // Test putAll

    @Test
    public void testPutAll_EmptyTableStillEmpty() {
        table.putAll(ImmutableTable.<String, String, String>of());
        assertTrue(table.isEmpty());
    }

    @Test
    public void testPutAll_AllEntriesInserted() {
        table.putAll(ImmutableTable.<String, String, String>builder()
                .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                .put("row1", "column1", "value1")
                .build()
        );

        assertEquals(2, table.size());
    }

    @Test
    public void testPutAll_ExistingValueOverwritten() {
        table.putAll(ImmutableTable.<String, String, String>builder()
                        .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                        .put("row1", "column1", "value1")
                        .build()
        );

        assertEquals(2, table.size());

        // Reinsert same rows, they should overwrite existing
        table.putAll(ImmutableTable.<String, String, String>builder()
                        .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                        .put("row1", "column1", "value1")
                        .build()
        );

        assertEquals(2, table.size());
    }

    // Test get

    @Test
    public void testGet_NonExistingRowIsNull() {
        assertNull(table.get(TEST_ROW, TEST_COLUMN));
    }

    @Test
    public void testGet_ExistingRowReturnsValue() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(TEST_VALUE, table.get(TEST_ROW, TEST_COLUMN));
    }

    // Test rowKeySet

    @Test
    public void testRowKeySet_EmptyTableHasNoRowKeys() {
        assertTrue(table.rowKeySet().isEmpty());
    }

    @Test
    public void testRowKeySet_TableWithSingleRowHasExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.rowKeySet().size());
        assertEquals(TEST_ROW, Iterables.getOnlyElement(table.rowKeySet()));
    }

    @Test
    public void testRowKeySet_ClearRemovesRows() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.rowKeySet().clear();
        assertTrue(table.isEmpty());
    }

    @Test
    public void testRowKeySet_RemoveRemovesRows() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.rowKeySet().remove(TEST_ROW);
        assertTrue(table.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRowKeySet_AddRowIsNotSupported() {
        table.rowKeySet().add(TEST_ROW);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRowKeySet_AddAllRowIsNotSupported() {
        table.rowKeySet().addAll(ImmutableSet.of(TEST_ROW));
    }

    @Test
    public void testRowKeySet_DuplicateRowKeysIgnored() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);

        assertEquals(1, table.rowKeySet().size());
    }

    @Test
    public void testRowKeySet_ContainsExpected() {
        assertFalse(table.rowKeySet().contains(TEST_ROW));

        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertTrue(table.rowKeySet().contains(TEST_ROW));
        assertFalse(table.rowKeySet().contains("row1"));
    }

    @Test
    public void testRowKeySet_RemoveAllRemovesExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put("row2", TEST_COLUMN, TEST_VALUE);
        table.put("row3", TEST_COLUMN, TEST_VALUE);

        table.rowKeySet().removeAll(ImmutableSet.of(TEST_ROW));
        assertEquals(3, table.size());
    }

    @Test
    public void testRowKeySet_RetainAllRemovesExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put("row2", TEST_COLUMN, TEST_VALUE);
        table.put("row3", TEST_COLUMN, TEST_VALUE);

        table.rowKeySet().retainAll(ImmutableSet.of(TEST_ROW));
        assertEquals(2, table.size());
    }

    @Test
    public void testRowKeySet_ContainsAllExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put("row2", TEST_COLUMN, TEST_VALUE);
        table.put("row3", TEST_COLUMN, TEST_VALUE);

        assertTrue(table.rowKeySet().containsAll(ImmutableList.of(TEST_ROW, TEST_ROW, "row1")));
        assertTrue(table.rowKeySet().containsAll(ImmutableList.of(TEST_ROW, "row1")));
        assertTrue(table.rowKeySet().containsAll(Collections.emptySet()));
        assertFalse(table.rowKeySet().containsAll(ImmutableList.of("invalid")));
        assertFalse(table.rowKeySet().containsAll(ImmutableList.of(TEST_ROW, "invalid")));
    }

    // Test columnKeySet

    @Test
    public void testColumnKeySet_EmptyTableHasNoColumnKeys() {
        assertTrue(table.columnKeySet().isEmpty());
    }

    @Test
    public void testColumnKeySet_TableWithSingleColumnHasExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.columnKeySet().size());
        assertEquals(TEST_COLUMN, Iterables.getOnlyElement(table.columnKeySet()));
    }

    @Test
    public void testColumnKeySet_ClearRemovesColumns() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.columnKeySet().clear();
        assertTrue(table.isEmpty());
    }

    @Test
    public void testColumnKeySet_RemoveRemovesColumns() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.columnKeySet().remove(TEST_COLUMN);
        assertTrue(table.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testColumnKeySet_AddColumnIsNotSupported() {
        table.columnKeySet().add(TEST_COLUMN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testColumnKeySet_AddAllColumnIsNotSupported() {
        table.columnKeySet().addAll(ImmutableSet.of(TEST_COLUMN));
    }

    @Test
    public void testColumnKeySet_DuplicateColumnKeysIgnored() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.columnKeySet().size());
    }

    @Test
    public void testColumnKeySet_ContainsExpected() {
        assertFalse(table.columnKeySet().contains(TEST_ROW));

        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertTrue(table.columnKeySet().contains(TEST_COLUMN));
        assertFalse(table.columnKeySet().contains("column1"));
    }

    @Test
    public void testColumnKeySet_RemoveAllRemovesExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put(TEST_ROW, "column2", TEST_VALUE);
        table.put(TEST_ROW, "column3", TEST_VALUE);

        table.columnKeySet().removeAll(ImmutableSet.of(TEST_COLUMN));
        assertEquals(3, table.size());
    }

    @Test
    public void testColumnKeySet_RetainAllRemovesExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put(TEST_ROW, "column2", TEST_VALUE);
        table.put(TEST_ROW, "column3", TEST_VALUE);

        table.columnKeySet().retainAll(ImmutableSet.of(TEST_COLUMN));
        assertEquals(2, table.size());
    }

    @Test
    public void testColumnKeySet_ContainsAllExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put(TEST_ROW, "column2", TEST_VALUE);
        table.put(TEST_ROW, "column3", TEST_VALUE);

        assertTrue(table.columnKeySet().containsAll(ImmutableList.of(TEST_COLUMN, TEST_COLUMN, "column1")));
        assertTrue(table.columnKeySet().containsAll(ImmutableList.of(TEST_COLUMN, "column1")));
        assertTrue(table.columnKeySet().containsAll(Collections.emptySet()));
        assertFalse(table.columnKeySet().containsAll(ImmutableList.of("invalid")));
        assertFalse(table.columnKeySet().containsAll(ImmutableList.of(TEST_COLUMN, "invalid")));
    }

    // Test remove

    @Test
    public void testRemove_RemoveNonExistingRow() {
        assertNull(table.remove(TEST_ROW, TEST_COLUMN));
    }

    @Test
    public void testRemove_RemovedRowIsGone() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertNotNull(table.remove(TEST_ROW, TEST_COLUMN));
        assertTrue(table.isEmpty());
    }

    @Test
    public void testRemove_RemovedRowIsReturned() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(TEST_VALUE, table.remove(TEST_ROW, TEST_COLUMN));
    }

    // Test row

    @Test
    public void testRow_EmptyRowIsEmpty() {
        assertTrue(table.row(TEST_ROW).isEmpty());
    }

    @Test
    public void testRow_SingleRowIsNotEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.row(TEST_ROW).isEmpty());
        assertTrue(table.row("row1").isEmpty());
    }

    @Test
    public void testRow_RemoveEntry() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.row(TEST_ROW).remove(TEST_COLUMN);
        assertTrue(table.isEmpty());
    }

    @Test
    public void testRow_PutUpdatesTable() {
        table.row(TEST_ROW).put(TEST_COLUMN, TEST_VALUE);
        assertEquals(1, table.size());
    }

    @Test
    public void testRow_ClearUpdatesTable() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.row(TEST_ROW).clear();
        assertTrue(table.isEmpty());
    }

    @Test
    public void testRow_NoInteraction() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.row("row1").clear();
        assertFalse(table.isEmpty());
    }

    @Test
    public void testRow_SingleRowReturnsExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        final Map<String, String> row = table.row(TEST_ROW);
        assertEquals(TEST_COLUMN, Iterables.getOnlyElement(row.keySet()));
        assertEquals(TEST_VALUE, Iterables.getOnlyElement(row.values()));
    }

    // Test column

    @Test
    public void testColumn_EmptyColumnIsEmpty() {
        assertTrue(table.column(TEST_COLUMN).isEmpty());
    }

    @Test
    public void testColumn_SingleColumnIsNotEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.column(TEST_COLUMN).isEmpty());
        assertTrue(table.column("column1").isEmpty());
    }

    @Test
    public void testColumn_RemoveEntry() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        table.column(TEST_COLUMN).remove(TEST_ROW);
        assertTrue(table.isEmpty());
    }

    @Test
    public void testColumn_PutUpdatesTable() {
        table.column(TEST_COLUMN).put(TEST_ROW, TEST_VALUE);
        assertEquals(1, table.size());
    }

    @Test
    public void testColumn_ClearUpdatesTable() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.column(TEST_COLUMN).clear();
        assertTrue(table.isEmpty());
    }

    @Test
    public void testColumn_NoInteraction() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.column("column1").clear();
        assertFalse(table.isEmpty());
    }

    @Test
    public void testColumn_SingleColumnReturnsExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column2", TEST_VALUE);

        final Map<String, String> column = table.column(TEST_COLUMN);
        assertEquals(TEST_ROW, Iterables.getOnlyElement(column.keySet()));
        assertEquals(TEST_VALUE, Iterables.getOnlyElement(column.values()));
    }

    // Test rowMap

    @Test
    public void testRowMap_EmptyTableHasEmptyRowMap() {
        assertTrue(table.rowMap().isEmpty());
    }

    @Test
    public void testRowMap_SingleEntryHasSingleEntryInRowMap() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.rowMap().size());
    }

    @Test
    public void testRowMap_MapIsGroupedByRow() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(2, table.rowMap().size());
    }

    // Test columnMap

    @Test
    public void testColumnMap_EmptyTableHasEmptyColumnMap() {
        assertTrue(table.columnMap().isEmpty());
    }

    @Test
    public void testColumnMap_SingleEntryHasSingleEntryInColumnMap() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.columnMap().size());
    }

    @Test
    public void testColumnMap_MapIsGroupedByColumn() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);

        assertEquals(2, table.columnMap().size());
    }

    // Test values

    @Test
    public void testValues_EmptyTableHasNoValues() {
        assertTrue(table.values().isEmpty());
    }

    @Test
    public void testValues_ReturnsDuplicateValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);

        assertEquals(2, table.values().size());
    }

    // Test cellSet

    @Test
    public void testCellSet_EmptyTableHasNoCells() {
        assertTrue(table.cellSet().isEmpty());
    }

    @Test
    public void testCellSet_SingleCell() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.cellSet().isEmpty());

        final Table.Cell<String, String, String> cell = Iterables.getOnlyElement(table.cellSet());
        assertEquals(TEST_ROW, cell.getRowKey());
        assertEquals(TEST_COLUMN, cell.getColumnKey());
        assertEquals(TEST_VALUE, cell.getValue());
    }

    // Test load

    @Test
    public void testMany_ManyRows() {
        for (int i = 0; i < 1000; i++) {
            table.put(String.valueOf(i), TEST_COLUMN, randomString(100));
        }

        assertEquals(1000, table.size());
        assertEquals(1000, table.column(TEST_COLUMN).size());
        assertEquals(1000, table.values().size());
    }

    @Test
    public void testMany_ManyColumns() {
        for (int i = 0; i < 1000; i++) {
            table.put(TEST_ROW, String.valueOf(i), randomString(100));
        }

        assertEquals(1000, table.size());
        assertEquals(1000, table.row(TEST_ROW).size());
        assertEquals(1000, table.values().size());
    }

    // Test multiple tables

    @Test
    public void testMultipleTables_NoInteraction() {
        final Table<String, String, String> table2 = createTable("test2", handle);

        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertFalse(table.isEmpty());
        assertTrue(table2.isEmpty());
    }
}
