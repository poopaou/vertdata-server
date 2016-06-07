package com.medimob.data;

/**
 * Created by cyrille on 23/04/16.
 */
public final class DocChange {

    private DocChange() {
        throw new IllegalStateException("No instances !!");
    }

    public static final String REV = "_rev";
    public static final String DATE = "_date";
    public static final String DIFFS = "_diffs";
    public static final String CONFLICT = "_conflict";
}
