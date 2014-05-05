package com.jamierf.dbtable;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.util.Iterator;
import java.util.Set;

class DbSet extends DbCollection implements Set<byte[]> {

    DbSet(String tableName, String valueField, Handle handle) {
        super (tableName, valueField, handle);
    }

    @Override
    public int size() {
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s", tableName, valueField))
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public Iterator<byte[]> iterator() {
        return handle.createQuery(String.format("SELECT DISTINCT %2$s FROM %1$s", tableName, valueField))
                .map(ByteArrayMapper.FIRST)
                .iterator();
    }
}
