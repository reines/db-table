package com.jamierf.dbtable.util.container;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.ContainerBuilder;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContainerBuilderTest {

    private ContainerBuilder<Map<String, String>> mapContainerBuilder;
    private ContainerBuilder<Table<String, String, String>> tableContainerBuilder;

    @Before
    public void setUp() {
        mapContainerBuilder = MapContainerBuilder.getInstance();
        tableContainerBuilder = TableContainerBuilder.getInstance();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMap_NullEntry() {
        mapContainerBuilder.add(null);
    }

    @Test
    public void testMap_Empty() {
        assertTrue(mapContainerBuilder.build().isEmpty());
    }

    @Test
    public void testMap_ValidMapEntry() {
        mapContainerBuilder.add(Maps.immutableEntry("key", "value"));
        assertEquals(1, mapContainerBuilder.build().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTable_NullEntry() {
        tableContainerBuilder.add(null);
    }

    @Test
    public void testTable_Empty() {
        assertTrue(tableContainerBuilder.build().isEmpty());
    }

    @Test
    public void testTable_ValidTableCell() {
        tableContainerBuilder.add(Tables.immutableCell("row", "column", "value"));
        assertEquals(1, tableContainerBuilder.build().size());
    }
}
