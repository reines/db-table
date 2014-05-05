package com.jamierf.dbtable;

import com.google.common.collect.Collections2;
import com.jamierf.dbtable.util.filter.UniqueByteArraysFilter;
import com.jamierf.dbtable.util.sql.InClauseArgumentList;
import com.yammer.collections.transforming.TypedPredicate;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

public class DbCollection extends AbstractCollection<byte[]> {

    protected final String tableName;
    protected final String valueField;
    protected final Handle handle;

    DbCollection(String tableName, String valueField, Handle handle) {
        this.tableName = tableName;
        this.valueField = valueField;
        this.handle = handle;
    }

    @Override
    public int size() {
        return handle.createQuery(String.format("SELECT COUNT(%2$s) FROM %1$s", tableName, valueField))
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s", tableName))
                .first() == null;
    }

    @Override
    public boolean contains(Object value) {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s = :%2$s", tableName, valueField))
                .bind(valueField, value)
                .map(IntegerMapper.FIRST)
                .first() != null;
    }
    
    @Override
    public Iterator<byte[]> iterator() {
        return handle.createQuery(String.format("SELECT %2$s FROM %1$s", tableName, valueField))
                .map(ByteArrayMapper.FIRST)
                .iterator();
    }

    @Override
    public boolean remove(Object value) {
        final boolean result = contains(value);
        handle.execute(String.format("DELETE FROM %1$s WHERE %2$s = :%2$s", tableName, valueField), value);
        return result;
    }

    @Override
    public boolean containsAll(@Nullable Collection<?> values) {
        // Filter out non-unique values so the count for DISTINCT matches
        final Collection<?> uniqueValues = Collections2.filter(values, TypedPredicate.wrap(new UniqueByteArraysFilter()));

        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, uniqueValues);
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s WHERE %2$s IN(%3$s)", tableName, valueField, in.toSql()))
                .bindFromMap(in.toMap())
                .map(IntegerMapper.FIRST)
                .first() == in.size();
    }

    @Override
    public boolean retainAll(@Nullable Collection<?> values) {
        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, values);
        return handle.update(String.format("DELETE FROM %1$s WHERE %2$s NOT IN (%3$s)", tableName, valueField, in.toSql()), in.toArguments()) > 0;
    }

    @Override
    public boolean removeAll(@Nullable Collection<?> values) {
        final InClauseArgumentList<?> in = new InClauseArgumentList<>(valueField, values);
        return handle.update(String.format("DELETE FROM %1$s WHERE %2$s IN (%3$s)", tableName, valueField, in.toSql()), in.toArguments()) > 0;
    }

    @Override
    public void clear() {
        handle.execute(String.format("TRUNCATE TABLE %1$s", tableName));
    }
}
