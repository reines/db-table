package com.jamierf.dbtable;

import com.google.common.base.Joiner;
import com.jamierf.dbtable.util.mapper.selection.SelectionMap;
import com.jamierf.dbtable.util.mapper.selection.AbstractSelectionMapFactory;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.util.Iterator;
import java.util.Set;

class DbSet<T> extends DbCollection<T> implements Set<T> {

    DbSet(String tableName, Handle handle, SelectionMap selectionMap, AbstractSelectionMapFactory<T> selectionMapFactory) {
        super (tableName, handle, selectionMap, selectionMapFactory);
    }

    @Override
    public int size() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.fields());
        return handle.createQuery(String.format("SELECT COUNT(DISTINCT %2$s) FROM %1$s WHERE %3$s", tableName, fieldsToCount, selectionMap.asSql()))
                .bindFromMap(selectionMap.asMap())
                .map(IntegerMapper.FIRST)
                .first();
    }

    @Override
    public Iterator<T> iterator() {
        final String fieldsToCount = Joiner.on(", ").join(selectionMapFactory.fields());
        return handle.createQuery(String.format("SELECT DISTINCT %2$s FROM %1$s WHERE " + selectionMap.asSql(), tableName, fieldsToCount))
                .bindFromMap(selectionMap.asMap())
                .map(selectionMapFactory.mapper())
                .iterator();
    }
}
