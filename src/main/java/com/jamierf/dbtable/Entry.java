package com.jamierf.dbtable;

import java.util.Arrays;

public class Entry {

    private final byte[] payload;

    public Entry(byte[] payload) {
        this.payload = payload;
    }

    public byte[] toBytes() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        if (!Arrays.equals(payload, entry.payload)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return payload != null ? Arrays.hashCode(payload) : 0;
    }

    @Override
    public String toString() {
        return Arrays.toString(payload);
    }
}
