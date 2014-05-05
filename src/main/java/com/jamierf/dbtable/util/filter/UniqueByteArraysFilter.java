package com.jamierf.dbtable.util.filter;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;

public class UniqueByteArraysFilter implements Predicate<byte[]> {

    private static class Blob {

        private final byte[] bytes;

        private Blob(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Blob blob = (Blob) o;

            if (!Arrays.equals(bytes, blob.bytes)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return bytes != null ? Arrays.hashCode(bytes) : 0;
        }
    }

    private final Set<Blob> blobs;

    public UniqueByteArraysFilter() {
        blobs = Sets.newHashSet();
    }

    @Override
    public boolean apply(@Nullable byte[] input) {
        return blobs.add(new Blob(input));
    }
}
