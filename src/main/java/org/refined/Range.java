package org.refined;

public final class Range {
    public static Integer[] of(int startInclusive,int endInclusive) {
        Integer[] res = new Integer[endInclusive-startInclusive + 1];
        for (int i = 0; i < res.length; i++) {
            res[i] = startInclusive++;
        }
        return res;
    }
}
