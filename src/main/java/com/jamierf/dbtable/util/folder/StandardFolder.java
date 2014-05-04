package com.jamierf.dbtable.util.folder;

import org.skife.jdbi.v2.ContainerBuilder;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.StatementContext;

import java.sql.SQLException;

public class StandardFolder<A extends ContainerBuilder, T> implements Folder3<A, T> {

    public static <A extends ContainerBuilder, T> Folder3<A, T> getInstance() {
        return new StandardFolder<>();
    }

    @Override
    public A fold(A accumulator, T rs, FoldController control, StatementContext ctx) throws SQLException {
        accumulator.add(rs);
        return accumulator;
    }
}
