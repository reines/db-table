package com.jamierf.dbtable;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;

import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class BaseDbTableTest {

    private static final String DATABASE_NAME = "test";
    private static final byte[] TEST_ROW = "row".getBytes();
    private static final byte[] TEST_COLUMN = "column".getBytes();
    private static final byte[] TEST_VALUE = "value".getBytes();

    private static final Random RANDOM = new Random();

    private static byte[] randomBytes(int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static byte[] toBytes(Number value) {
        return new BigInteger(value.toString()).toByteArray();
    }

    private DBI dbi;
    private BaseDbTable table;

    @Before
    public void setUp() {
        dbi = new DBI("jdbc:h2:mem:test");

        table = new BaseDbTable(DATABASE_NAME, dbi);
        System.out.println("Starting, created table: " + table);
    }

    @After
    public void tearDown() {
        System.out.println("Finished, destroying table: " + table);
        table.delete();
    }

    @Test
    public void testCreate_CreatesTableWithoutException() {
        assertNotNull(table);
    }

    @Test
    public void testEmpty_EmptyTableIsEmpty() {
        assertTrue(table.isEmpty());
    }

    @Test
    public void testEmpty_NotEmptyTableIsntEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.isEmpty());
    }

    @Test
    public void testSize_EmptyTableIsZero() {
        assertEquals(0, table.size());
    }

    @Test
    public void testSize_OneRowIsOne() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.size());
    }

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
        assertFalse(table.contains("bla".getBytes(), "bla".getBytes()));
        assertFalse(table.containsRow("bla".getBytes()));
        assertFalse(table.containsColumn("bla".getBytes()));
        assertFalse(table.containsValue("bla".getBytes()));
    }

    @Test
    public void testClear_ClearedTableIsEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.clear();

        assertTrue(table.isEmpty());
    }

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

    @Test
    public void testPutAll_EmptyTableStillEmpty() {
        table.putAll(ImmutableTable.<byte[], byte[], byte[]>of());
        assertTrue(table.isEmpty());
    }

    @Test
    public void testPutAll_AllEntriesInserted() {
        table.putAll(ImmutableTable.<byte[], byte[], byte[]>builder()
                .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                .put("row1".getBytes(), "column1".getBytes(), "value1".getBytes())
                .build()
        );

        assertEquals(2, table.size());
    }

    @Test
    public void testPutAll_ExistingValueOverwritten() {
        table.putAll(ImmutableTable.<byte[], byte[], byte[]>builder()
                        .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                        .put("row1".getBytes(), "column1".getBytes(), "value1".getBytes())
                        .build()
        );

        assertEquals(2, table.size());

        // Reinsert same rows, they should overwrite existing
        table.putAll(ImmutableTable.<byte[], byte[], byte[]>builder()
                        .put(TEST_ROW, TEST_COLUMN, TEST_VALUE)
                        .put("row1".getBytes(), "column1".getBytes(), "value1".getBytes())
                        .build()
        );

        assertEquals(2, table.size());
    }

    @Test
    public void testGet_NonExistingRowIsNull() {
        assertNull(table.get(TEST_ROW, TEST_COLUMN));
    }

    @Test
    public void testGet_ExistingRowReturnsValue() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertArrayEquals(TEST_VALUE, table.get(TEST_ROW, TEST_COLUMN));
    }

    @Test
    public void testKeySet_EmptyTableHasNoRowKeys() {
        assertTrue(table.rowKeySet().isEmpty());
        assertTrue(table.columnKeySet().isEmpty());
    }

    @Test
    public void testKeySet_TableWithSingleRowHasExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(1, table.rowKeySet().size());
        assertArrayEquals(TEST_ROW, Iterables.getOnlyElement(table.rowKeySet()));

        assertEquals(1, table.columnKeySet().size());
        assertArrayEquals(TEST_COLUMN, Iterables.getOnlyElement(table.columnKeySet()));
    }

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

        assertArrayEquals(TEST_VALUE, table.remove(TEST_ROW, TEST_COLUMN));
    }

    @Test
    public void testRow_EmptyRowIsEmpty() {
        assertTrue(table.row(TEST_ROW).isEmpty());
    }

    @Test
    public void testRow_SingleRowIsNotEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.row(TEST_ROW).isEmpty());
        assertTrue(table.row("row1".getBytes()).isEmpty());
    }

    @Test
    public void testRow_SingleRowReturnsExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1".getBytes(), TEST_COLUMN, TEST_VALUE);

        final Map<byte[], byte[]> row = table.row(TEST_ROW);
        assertArrayEquals(TEST_COLUMN, Iterables.getOnlyElement(row.keySet()));
        assertArrayEquals(TEST_VALUE, Iterables.getOnlyElement(row.values()));
    }

    @Test
    public void testColumn_EmptyColumnIsEmpty() {
        assertTrue(table.column(TEST_COLUMN).isEmpty());
    }

    @Test
    public void testColumn_SingleColumnIsNotEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.column(TEST_COLUMN).isEmpty());
        assertTrue(table.column("column1".getBytes()).isEmpty());
    }

    @Test
    public void testColumn_SingleColumnReturnsExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column2".getBytes(), TEST_VALUE);

        final Map<byte[], byte[]> column = table.column(TEST_COLUMN);
        assertArrayEquals(TEST_ROW, Iterables.getOnlyElement(column.keySet()));
        assertArrayEquals(TEST_VALUE, Iterables.getOnlyElement(column.values()));
    }

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
    public void testRowMap_RowMapIsGroupedByRow() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1".getBytes(), TEST_VALUE);

        assertEquals(2, table.rowMap().size());
    }

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
    public void testValues_EmptyTableHasNoValues() {
        assertTrue(table.values().isEmpty());
    }

    @Test
    public void testValues_ReturnsDuplicateValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1".getBytes(), TEST_VALUE);

        assertEquals(2, table.values().size());
    }

    @Test
    public void testColumnMap_RowMapIsGroupedByColumn() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1".getBytes(), TEST_COLUMN, TEST_VALUE);

        assertEquals(2, table.columnMap().size());
    }

    @Test
    public void testCellSet_EmptyTableHasNoCells() {
        assertTrue(table.cellSet().isEmpty());
    }

    @Test
    public void testCellSet_SingleCell() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.cellSet().isEmpty());

        final Table.Cell<byte[], byte[], byte[]> cell = Iterables.getOnlyElement(table.cellSet());
        assertArrayEquals(TEST_ROW, cell.getRowKey());
        assertArrayEquals(TEST_COLUMN, cell.getColumnKey());
        assertArrayEquals(TEST_VALUE, cell.getValue());
    }

    @Test
    public void testMany_ManyRows() {
        for (int i = 0; i < 1000; i++) {
            table.put(toBytes(i), TEST_COLUMN, randomBytes(100));
        }

        assertEquals(1000, table.size());
        assertEquals(1000, table.column(TEST_COLUMN).size());
        assertEquals(1000, table.values().size());
    }

    @Test
    public void testMany_ManyColumns() {
        for (int i = 0; i < 1000; i++) {
            table.put(TEST_ROW, toBytes(i), randomBytes(100));
        }

        assertEquals(1000, table.size());
        assertEquals(1000, table.row(TEST_ROW).size());
        assertEquals(1000, table.values().size());
    }

    @Test
    public void testDelete_DeleteClosesConnection() {
        table.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void testDelete_UnableToUseAfterDelete() {
        table.delete();

        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
    }

    @Test
    public void testMultipleTables_NoInteraction() {
        final BaseDbTable table2 = new BaseDbTable("test2", dbi);

        try {
            table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
            assertTrue(table2.isEmpty());
        }
        finally {
            table2.delete();
        }
    }
}
