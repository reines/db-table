package com.jamierf.dbtable.util.mapper;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class TableCellMapper<R, C, V> implements ResultSetMapper<Table.Cell<R, C, V>> {

    @Override
    public Table.Cell<R, C, V> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return Tables.immutableCell(
                mapRow(index, r, ctx),
                mapColumn(index, r, ctx),
                mapValue(index, r, ctx)
        );
    }

    protected abstract R mapRow(int index, ResultSet r, StatementContext ctx) throws SQLException;
    protected abstract C mapColumn(int index, ResultSet r, StatementContext ctx) throws SQLException;
    protected abstract V mapValue(int index, ResultSet r, StatementContext ctx) throws SQLException;
}
