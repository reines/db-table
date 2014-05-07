package com.jamierf.dbtable.core.mapper.selection;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jamierf.dbtable.core.sql.InClauseArgumentList;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class SelectionMap {

    public static final SelectionMap NONE = new SelectionMap(Collections.<String, Object>emptyMap());

    public static SelectionMap of(String key, Object value) {
        return new SelectionMap(ImmutableMap.of(key, value));
    }

    private static final String SQL_TEMPLATE = "(%s)";
    private static final Joiner CONDITION_JOINER = Joiner.on(" AND ");
    private static final String EQUALS_CONDITION_TEMPLATE = "%1$s = :%1$s";
    private static final String IN_CONDITION_TEMPLATE = "%1$s IN (%2$s)";
    private static final String EMPTY_CONDITION = "1";

    private final Map<String, Object> mapping;
    private final Collection<String> conditions;

    @SuppressWarnings("unchecked")
    public SelectionMap(Map<String, Object> selection) {
        mapping = Maps.newHashMap();
        conditions = Sets.newHashSet();

        for (Map.Entry<String, Object> input : selection.entrySet()) {
            if (input.getValue() instanceof Iterable) {
                final InClauseArgumentList<Object> in = new InClauseArgumentList<>("bla", (Iterable<Object>) input.getValue());
                conditions.add(String.format(IN_CONDITION_TEMPLATE, input.getKey(), in.asSql()));
                mapping.putAll(in.asMap());
            }
            else {
                conditions.add(String.format(EQUALS_CONDITION_TEMPLATE, input.getKey()));
                mapping.put(input.getKey(), input.getValue());
            }
        }

        if (conditions.isEmpty()) {
            conditions.add(EMPTY_CONDITION);
        }
    }

    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(mapping);
    }

    public String asSql() {
        return String.format(SQL_TEMPLATE, CONDITION_JOINER.join(conditions));
    }
}
