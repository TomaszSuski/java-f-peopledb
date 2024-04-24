package com.lingarogroup.peopledb.annotation;

import com.lingarogroup.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation is used to associate a SQL query with a CRUD operation.
 * It is repeatable, meaning that multiple SQL queries can be associated with a single method.
 * It is retained at runtime, meaning that it can be accessed via reflection.
 *
 * @Retention This specifies that the annotation is available at runtime.
 * @Repeatable This specifies that the annotation can be applied multiple times to the same declaration or type use.
 */
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SQLContainer.class)
public @interface SQL {
    /**
     * This is the SQL query that is associated with the CRUD operation.
     *
     * @return The SQL query.
     */
    String value();

    /**
     * This is the type of the CRUD operation that the SQL query is associated with.
     *
     * @return The CRUD operation type.
     */
    CrudOperation operationType();
}
