package com.medimob.data;

/**
 * Created by cyrille on 18/03/16.
 */
public final class Change {

    /**
     * Document change id.
     * Type : {@link String}
     */
    public static final String DOCUMENT_ID = "docId";

    /**
     * Deleted flag.
     * Type : {@link Boolean}
     */
    public static final String DELETE = "deleted";

    /**
     * Conflict flag.
     * Type : {@link Boolean}
     */
    public static final String CONFLICT = "confict";

    /**
     * Update flag.
     * Type : {@link Boolean}
     */
    public static final String UPDATE = "update";

    /**
     * Insert flag.
     * Type : {@link Boolean}
     */
    public static final String INSERT = "insert";

    private Change() {
        throw new IllegalStateException("No instance !");
    }
}
