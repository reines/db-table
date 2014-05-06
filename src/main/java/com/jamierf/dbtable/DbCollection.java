package com.jamierf.dbtable;

import com.google.common.base.Joiner;
import com.jamierf.dbtable.mapper.selection.AbstractSelectionMapFactory;
import com.jamierf.dbtable.mapper.selection.SelectionMap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

class DbCollection<T> extends AbstractCollection<T> {

    protected final String tableName;
    protected final Handle handle;
    protected final SelectionMap selectionMap;
    protected final AbstractSelectionMapFactory<T> selectionMapFactory;
    protected final ResultSetMapper<T> fieldMapper;

    DbCollection(String tableName, Handle handle, SelectionMap selectionMap, AbstractSelectionMapFactory<T> selectionMapFactory, ResultSetMapper<T> fieldMapper) {
        this.tableName = tableName;
        this.handle = handle;
        this.selectionMap = selectionMap;
        this.selectionMapFactory = selectionMapFactory;
        this.fieldMapper = fieldMapper;
    }

    @Override
    public int size() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.keyFields());
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
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.keyFields());
        return handle.createQuery(String.format("SELECT %2$s FROM %1$s WHERE %3$s", tableName, fieldsToCount, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .map(fieldMapper)
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
