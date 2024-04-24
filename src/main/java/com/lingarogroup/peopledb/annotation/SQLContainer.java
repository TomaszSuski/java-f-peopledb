package com.lingarogroup.peopledb.annotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;

/**
 * This annotation is used to hold an array of SQL annotations.
 * It is retained at runtime, meaning that it can be accessed via reflection.
 *
 * @Retention This specifies that the annotation is available at runtime.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface SQLContainer {
    /**
     * This method returns an array of SQL annotations.
     * Each SQL annotation associates a SQL query with a CRUD operation.
     *
     * @return An array of SQL annotations.
     */
    SQL[] value();
}
