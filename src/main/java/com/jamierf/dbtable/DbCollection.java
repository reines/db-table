package com.jamierf.dbtable;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.jamierf.dbtable.util.mapper.selection.SelectionMap;
import com.jamierf.dbtable.util.filter.UniqueByteArraysFilter;
import com.jamierf.dbtable.util.mapper.selection.AbstractSelectionMapFactory;
import com.yammer.collections.transforming.TypedPredicate;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

class DbCollection<T> extends AbstractCollection<T> {

    protected final String tableName;
    protected final Handle handle;
    protected final SelectionMap selectionMap;
    protected final AbstractSelectionMapFactory<T> selectionMapFactory;

    DbCollection(String tableName, Handle handle, SelectionMap selectionMap, AbstractSelectionMapFactory<T> selectionMapFactory) {
        this.tableName = tableName;
        this.handle = handle;
        this.selectionMap = selectionMap;
        this.selectionMapFactory = selectionMapFactory;
    }

    @Override
    public int size() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.fields());
        return handle.createQuery(String.format("SELECT COUNT(%2$s) FROM %1$s WHERE %3$s", tableName, fieldsToCount, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public boolean isEmpty() {
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s ", tableName, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .first() == null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object value) {
        final SelectionMap valueMap = selectionMapFactory.get(value);
        return handle.createQuery(String.format("SELECT 1 FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .map(IntegerMapper.FIRST)
                .first() != null;
    }

    @Override
    public Iterator<T> iterator() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.fields());
        return handle.createQuery(String.format("SELECT %2$s FROM %1$s WHERE %3$s", tableName, fieldsToCount, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .map(selectionMapFactory.mapper())
                .iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object value) {
        final boolean result = contains(value);

        final SelectionMap valueMap = selectionMapFactory.get(value);
        handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .execute();

        return result;
    }

    @Override
    public boolean containsAll(@Nullable Collection<?> values) {
        // Filter out non-unique values so the count for DISTINCT matches
        final Set<?> uniqueValues = ImmutableSet.copyOf(Collections2.filter(values, TypedPredicate.wrap(new UniqueByteArraysFilter())));

        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.fields());
        final SelectionMap valueMap = selectionMapFactory.get(uniqueValues);
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s WHERE %3$s AND %4$s", tableName, fieldsToCount, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .map(IntegerMapper.FIRST)
                .first() == uniqueValues.size();
    }

    @Override
    public boolean retainAll(@Nullable Collection<?> values) {
        final SelectionMap valueMap = selectionMapFactory.get(values);
        return handle.createStatement(String.format("DELETE FROM %1$s WHERE NOT %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .execute() > 0;
    }

    @Override
    public boolean removeAll(@Nullable Collection<?> values) {
        final SelectionMap valueMap = selectionMapFactory.get(values);
        return handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s AND %3$s", tableName, valueMap.asSql(), selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .bindFromMap(valueMap.asMap())
                .execute() > 0;
    }

    @Override
    public void clear() {
        handle.createStatement(String.format("DELETE FROM %1$s WHERE %2$s", tableName, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .execute();
    }
}
