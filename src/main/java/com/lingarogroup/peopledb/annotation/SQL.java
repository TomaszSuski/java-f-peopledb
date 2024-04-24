package com.lingarogroup.peopledb.annotation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SQLContainer.class)
public @interface SQL {
    String value();
    CrudOperation operationType();
}
