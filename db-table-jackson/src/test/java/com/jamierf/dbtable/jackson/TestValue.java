package com.jamierf.dbtable.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.Collection;
import java.util.Date;

public class TestValue {

    @JsonProperty
    private final Date date;

    @JsonProperty
    private final Collection<String> collection;

    @JsonProperty
    private final Optional<Long> optional;

    @JsonCreator
    public TestValue(
            @JsonProperty("date") Date date,
            @JsonProperty("collection") Collection<String> collection,
            @JsonProperty("optional") Optional<Long> optional) {
        this.date = date;
        this.collection = collection;
        this.optional = optional;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestValue testValue = (TestValue) o;

        if (collection != null ? !collection.equals(testValue.collection) : testValue.collection != null) return false;
        if (date != null ? !date.equals(testValue.date) : testValue.date != null) return false;
        if (optional != null ? !optional.equals(testValue.optional) : testValue.optional != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (collection != null ? collection.hashCode() : 0);
        result = 31 * result + (optional != null ? optional.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("date", date)
                .add("collection", collection)
                .add("optional", optional)
                .toString();
    }
}
