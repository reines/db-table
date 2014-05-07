package com.jamierf.dbtable.core;

import com.google.common.collect.*;
import com.jamierf.dbtable.core.util.StringCodec;
import com.yammer.collections.transforming.TransformingTable;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class DbTableTest {

    private static final String DATABASE_NAME = "test";
    private static final String TEST_ROW = "row";
    private static final String TEST_COLUMN = "column";
    private static final String TEST_VALUE = "value";

    private Handle handle;
    private Table<String, String, String> table;

    @Before
    public void setUp() {
        handle = DBI.open("jdbc:h2:mem:test");
        table = createTable(DATABASE_NAME);
    }

    private Table<String, String, String> createTable(String tableName) {
        return TransformingTable.create(
                new DbTable(tableName, handle),
                StringCodec.ENCODER, StringCodec.DECODER,
                StringCodec.ENCODER, StringCodec.DECODER,
                StringCodec.ENCODER, StringCodec.DECODER
        );
    }

    private void dropTable(String name) {
        handle.execute(String.format("DROP TABLE %s", name));
    }

    @After
    public void tearDown() {
        dropTable(DATABASE_NAME);
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
    public void testEmpty_NotEmptyTableIsNotEmpty() {
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
    public void testPut_PutsExpectedValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(TEST_ROW, Iterables.getOnlyElement(table.rowKeySet()));
        assertEquals(TEST_COLUMN, Iterables.getOnlyElement(table.columnKeySet()));
        assertEquals(TEST_VALUE, Iterables.getOnlyElement(table.values()));
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
    public void testRow_EntrySetSize() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(2, table.row(TEST_ROW).entrySet().size());
    }

    @Test
    public void testRow_KeySetSize() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(2, table.row(TEST_ROW).keySet().size());
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
    public void testRow_PutAllUpdatesTable() {
        table.row(TEST_ROW).putAll(ImmutableMap.of(
                TEST_COLUMN, TEST_VALUE,
                "column1", "value1"
        ));

        assertEquals(2, table.size());
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

    @Test
    public void testRow_EmptyEntrySetIsEmpty() {
        assertTrue(table.row(TEST_ROW).entrySet().isEmpty());
    }

    @Test
    public void testRow_EntrySetContainsEntries() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        final Map.Entry<String, String> entry = Iterables.getOnlyElement(table.row(TEST_ROW).entrySet());
        assertEquals(TEST_COLUMN, entry.getKey());
        assertEquals(TEST_VALUE, entry.getValue());

        assertTrue(table.row(TEST_ROW).entrySet().contains(Maps.immutableEntry(TEST_COLUMN, TEST_VALUE)));
        assertFalse(table.row(TEST_ROW).entrySet().contains(Maps.immutableEntry(TEST_COLUMN, "invalid")));
    }

    @Test
    public void testRow_ContainsExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.row(TEST_ROW).containsKey(TEST_COLUMN));
        assertFalse(table.row(TEST_ROW).containsKey("invalid"));
    }

    @Test
    public void testRow_ContainsExpectedValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.row(TEST_ROW).containsValue(TEST_VALUE));
        assertFalse(table.row(TEST_ROW).containsValue("invalid"));
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
    public void testColumn_EntrySetSize() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);

        assertEquals(2, table.column(TEST_COLUMN).entrySet().size());
    }

    @Test
    public void testColumn_KeySetSize() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);

        assertEquals(2, table.column(TEST_COLUMN).keySet().size());
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
    public void testColumn_PutAllUpdatesTable() {
        table.column(TEST_COLUMN).putAll(ImmutableMap.of(
                TEST_ROW, TEST_VALUE,
                "row1", "value1"
        ));

        assertEquals(2, table.size());
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

    @Test
    public void testColumn_EmptyEntrySetIsEmpty() {
        assertTrue(table.column(TEST_COLUMN).entrySet().isEmpty());
    }

    @Test
    public void testColumn_EntrySetContainsEntries() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        final Map.Entry<String, String> entry = Iterables.getOnlyElement(table.column(TEST_COLUMN).entrySet());
        assertEquals(TEST_ROW, entry.getKey());
        assertEquals(TEST_VALUE, entry.getValue());

        assertTrue(table.column(TEST_COLUMN).entrySet().contains(Maps.immutableEntry(TEST_ROW, TEST_VALUE)));
        assertFalse(table.column(TEST_COLUMN).entrySet().contains(Maps.immutableEntry(TEST_ROW, "invalid")));
    }

    @Test
    public void testColumn_ContainsExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.column(TEST_COLUMN).containsKey(TEST_ROW));
        assertFalse(table.column(TEST_COLUMN).containsKey("invalid"));
    }

    @Test
    public void testColumn_ContainsExpectedValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.column(TEST_COLUMN).containsValue(TEST_VALUE));
        assertFalse(table.column(TEST_COLUMN).containsValue("invalid"));
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

    @Test
    public void testRowMap_GetNotExistingRowIsNull() {
        assertNull(table.rowMap().get(TEST_ROW));
    }

    @Test
    public void testRowMap_GetExistingColumn() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(TEST_VALUE, table.rowMap().get(TEST_ROW).get(TEST_COLUMN));
    }

    @Test
    public void testRowMap_ContainsExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.rowMap().containsKey(TEST_ROW));
        assertFalse(table.rowMap().containsKey("invalid"));
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

    @Test
    public void testColumnMap_GetNotExistingColumnIsNull() {
        assertNull(table.columnMap().get(TEST_COLUMN));
    }

    @Test
    public void testColumnMap_GetExistingRow() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(TEST_VALUE, table.columnMap().get(TEST_COLUMN).get(TEST_ROW));
    }

    @Test
    public void testColumnMap_ContainsExpectedKeys() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.columnMap().containsKey(TEST_COLUMN));
        assertFalse(table.columnMap().containsKey("invalid"));
    }

    // Test values

    @Test
    public void testValues_EmptyTableHasNoValues() {
        assertTrue(table.values().isEmpty());
    }

    @Test
    public void testValues_NotEmptyWhenHasValues() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.values().isEmpty());
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
    public void testCellSet_NotEmptyWhenHasCells() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.cellSet().isEmpty());
    }

    @Test
    public void testCellSet_Size() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", TEST_COLUMN, TEST_VALUE);

        assertEquals(3, table.cellSet().size());
    }

    @Test
    public void testCellSet_EntrySetSize() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        table.put(TEST_ROW, "column1", TEST_VALUE);
        table.put("row1", "column1", TEST_VALUE);

        assertEquals(2, table.rowKeySet().size());
        assertEquals(2, table.columnKeySet().size());
    }

    @Test
    public void testCellSet_IterateFirstValue() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertEquals(Tables.immutableCell(TEST_ROW, TEST_COLUMN, TEST_VALUE), Iterables.getOnlyElement(table.cellSet()));
    }

    @Test
    public void testCellSet_SingleCell() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        final Set<Table.Cell<String, String, String>> cells = table.cellSet();
        assertFalse(cells.isEmpty());

        final Table.Cell<String, String, String> cell = Iterables.getOnlyElement(cells);
        assertEquals(TEST_ROW, cell.getRowKey());
        assertEquals(TEST_COLUMN, cell.getColumnKey());
        assertEquals(TEST_VALUE, cell.getValue());
    }

    @Test
    public void testCellSet_ContainsExpected() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);

        assertTrue(table.cellSet().contains(Tables.immutableCell(TEST_ROW, TEST_COLUMN, TEST_VALUE)));
        assertFalse(table.cellSet().contains(Tables.immutableCell(TEST_ROW, TEST_COLUMN, "invalid")));
    }

    // Test load

    private static final int TEST_ROW_COUNT = 1000;

    @Test
    public void testMany_ManyRows() {
        for (int i = 0; i < TEST_ROW_COUNT; i++) {
            table.put(String.valueOf(i), TEST_COLUMN, RandomStringUtils.random(100));
        }

        assertEquals(TEST_ROW_COUNT, table.size());
        assertEquals(TEST_ROW_COUNT, table.cellSet().size());
        assertEquals(TEST_ROW_COUNT, table.column(TEST_COLUMN).size());
        assertEquals(TEST_ROW_COUNT, table.values().size());
    }

    @Test
    public void testMany_ManyColumns() {
        for (int i = 0; i < TEST_ROW_COUNT; i++) {
            table.put(TEST_ROW, String.valueOf(i), RandomStringUtils.random(100));
        }

        assertEquals(TEST_ROW_COUNT, table.size());
        assertEquals(TEST_ROW_COUNT, table.cellSet().size());
        assertEquals(TEST_ROW_COUNT, table.row(TEST_ROW).size());
        assertEquals(TEST_ROW_COUNT, table.values().size());
    }

    private static final int MAX_KEY_LENGTH = 1024 * 128; // 128Kb
    private static final int MAX_VALUE_LENGTH = 1024 * 1024; // 1Mb

    @Test
    public void testLarge_LargeRowKey() {
        final String row = RandomStringUtils.random(MAX_KEY_LENGTH);
        table.put(row, TEST_COLUMN, TEST_VALUE);

        assertFalse(table.isEmpty());
        assertEquals(TEST_VALUE, table.get(row, TEST_COLUMN));
    }

    @Test
    public void testLarge_LargeColumnKey() {
        final String column = RandomStringUtils.random(MAX_KEY_LENGTH);
        table.put(TEST_ROW, column, TEST_VALUE);

        assertFalse(table.isEmpty());
        assertEquals(TEST_VALUE, table.get(TEST_ROW, column));
    }

    @Test
    public void testLarge_LargeValue() {
        final String value = RandomStringUtils.random(MAX_VALUE_LENGTH);
        table.put(TEST_ROW, TEST_COLUMN, value);

        assertFalse(table.isEmpty());
        assertEquals(value, table.get(TEST_ROW, TEST_COLUMN));
    }

    // Test multiple tables

    @Test
    public void testMultipleTables_NoInteraction() {
        final Table<String, String, String> table2 = createTable("test2");

        try {
            table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
            assertFalse(table.isEmpty());
            assertTrue(table2.isEmpty());
        }
        finally {
            dropTable("test2");
        }
    }
}
