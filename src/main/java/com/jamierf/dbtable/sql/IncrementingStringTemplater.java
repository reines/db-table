package com.jamierf.dbtable.sql;

import com.google.common.base.Supplier;

import java.util.concurrent.atomic.AtomicInteger;

public class IncrementingStringTemplater implements Supplier<String> {

    private final String template;
    private final AtomicInteger counter;

    public IncrementingStringTemplater(String template) {
        this.template = template;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public String get() {
        return String.format(template, counter.getAndIncrement());
    }
}
