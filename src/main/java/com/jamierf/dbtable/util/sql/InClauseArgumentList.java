package com.jamierf.dbtable.util.sql;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

public class InClauseArgumentList<T> {

    private static final StringTemplater ARGUMENT_PREFIXER = new StringTemplater(":%s");

    private final Map<String, T> values;

    @SuppressWarnings("unchecked")
    public InClauseArgumentList(String prefix, Iterable<T> values) {
        final Function<T, String> mapper = (Function<T, String>) Functions.forSupplier(new IncrementingStringTemplater(prefix + "_%d"));
        this.values = Maps.uniqueIndex(values, mapper);
    }

    public String asSql() {
        return Joiner.on(", ").join(Collections2.transform(values.keySet(), ARGUMENT_PREFIXER));
    }

    public Map<String, T> asMap() {
        return Collections.unmodifiableMap(values);
    }
}
