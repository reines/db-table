package com.jamierf.dbtable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.jamierf.dbtable.util.sql.InClauseArgumentList;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

class DbSet extends AbstractSet<byte[]> {

    private final String tableName;
    private final String valueField;
    private final Handle handle;

    DbSet(String tableName, String valueField, Handle handle) {
        this.tableName = tableName;
        this.valueField = valueField;
        this.handle = handle;
    }

    private Handle checkHandle() {
        Preconditions.checkState(handle != null, "This set has been closed.");
        return handle;
    }

    @Override
    public int size() {
        return checkHandle().createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s", tableName, valueField))
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %1$s", tableName))
                .first() == null;
    }

    @Override
    public boolean contains(Object value) {
        return checkHandle().createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s", tableName, valueField))
                .bind(valueField, value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException(); // TODO: Implement
    }

    @Override
    public boolean remove(Object value) {
        final boolean result = contains(value);
        checkHandle().execute(String.format("DELETE FROM %1$s WHERE %2$s = :%2$s", tableName, valueField), value);
        return result;
    }

    @Override
    public boolean containsAll(@Nullable Collection<?> values) {
        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, values);
        return checkHandle().createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s WHERE %2$s IN(%3$s)", tableName, valueField, in.toSql()))
                .bindFromMap(in.toMap())
                .map(IntegerMapper.FIRST)
                .first() == Sets.newHashSet(values).size();
    }

    @Override
    public boolean retainAll(@Nullable Collection<?> values) {
        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, values);
        return checkHandle().update(String.format("DELETE FROM %1$s WHERE %2$s NOT IN (%3$s)", tableName, valueField, in.toSql()), in.toArguments()) > 0;
    }

    @Override
    public boolean removeAll(@Nullable Collection<?> values) {
        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, values);
        return checkHandle().update(String.format("DELETE FROM %1$s WHERE %2$s IN (%3$s)", tableName, valueField, in.toSql()), in.toArguments()) > 0;
    }

    @Override
    public void clear() {
        checkHandle().execute(String.format("TRUNCATE TABLE %1$s", tableName));
    }
}
