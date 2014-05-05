package com.jamierf.dbtable.util.mapper.selection;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jamierf.dbtable.util.sql.InClauseArgumentList;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class SelectionMap {

    private static final String SQL_TEMPLATE = "(%s)";
    private static final Joiner CONDITION_JOINER = Joiner.on(" AND ");
    private static final String EMPTY_CONDITION = "1";

    public static SelectionMap of() {
        return new SelectionMap(Collections.<String, Object>emptyMap());
    }

    public static SelectionMap of(String k1, Object v1) {
        return new SelectionMap(ImmutableMap.of(k1, v1));
    }

    public static SelectionMap of(String k1, Object v1, String k2, Object v2) {
        return new SelectionMap(ImmutableMap.of(k1, v1, k2, v2));
    }

    public static SelectionMap of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return new SelectionMap(ImmutableMap.of(k1, v1, k2, v2, k3, v3));
    }

    private final Map<String, Object> mapping;
    private final Collection<String> sql;

    public SelectionMap(Map<String, Object> selection) {
        mapping = Maps.newHashMap();
        sql = Sets.newHashSet(EMPTY_CONDITION);

        for (Map.Entry<String, Object> input : selection.entrySet()) {
            if (input.getValue() instanceof Iterable) {
                final InClauseArgumentList<Object> in = new InClauseArgumentList<Object>("bla", (Iterable) input.getValue());
                sql.add(String.format("%1$s IN (%2$s)", input.getKey(), in.asSql()));
                mapping.putAll(in.asMap());
            }
            else {
                sql.add(String.format("%1$s = :%1$s", input.getKey()));
                mapping.put(input.getKey(), input.getValue());
            }
        }
    }

    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(mapping);
    }

    public String asSql() {
        return String.format(SQL_TEMPLATE, CONDITION_JOINER.join(sql));
    }
}
