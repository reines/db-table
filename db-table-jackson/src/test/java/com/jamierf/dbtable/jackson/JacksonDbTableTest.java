package com.jamierf.dbtable.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JacksonDbTableTest {

    private static final String DATABASE_NAME = "test";
    private static final String TEST_ROW = "elephant";
    private static final Integer TEST_COLUMN = 42;
    private static final TestValue TEST_VALUE = new TestValue(new Date(), ImmutableList.of("hello", "world"), Optional.of(Long.MAX_VALUE));
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new SmileFactory());

    static {
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

    private Handle handle;
    private Table<String, Integer, TestValue> table;

    @Before
    public void setUp() {
        handle = DBI.open("jdbc:h2:mem:test");
        table = createTable(DATABASE_NAME);
    }

    private Table<String, Integer, TestValue> createTable(String tableName) {
        return new JacksonDbTableBuilder(handle)
                .using(OBJECT_MAPPER)
                .build(tableName, String.class, Integer.class, TestValue.class);
    }

    private void dropTable(String name) {
        handle.execute(String.format("DROP TABLE %s", name));
    }

    @After
    public void tearDown() {
        dropTable(DATABASE_NAME);
        handle.close();
    }

    @Test
    public void testPut_TableIsNotEmpty() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertFalse(table.isEmpty());
    }

    @Test
    public void testGet_ReturnsExpectedValue() {
        table.put(TEST_ROW, TEST_COLUMN, TEST_VALUE);
        assertEquals(TEST_VALUE, table.get(TEST_ROW, TEST_COLUMN));
    }
}
