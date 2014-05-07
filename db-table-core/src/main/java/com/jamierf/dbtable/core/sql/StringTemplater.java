package com.jamierf.dbtable.core.sql;

import com.google.common.base.Function;

import javax.annotation.Nullable;

public class StringTemplater implements Function<String, String> {

    private final String template;

    public StringTemplater(String template) {
        this.template = template;
    }

    @Nullable
    @Override
    public String apply(@Nullable String input) {
        return String.format(template, input);
    }
}
