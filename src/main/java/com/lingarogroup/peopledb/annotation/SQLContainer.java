package com.lingarogroup.peopledb.annotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;

@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface SQLContainer {
    SQL[] value();
}
