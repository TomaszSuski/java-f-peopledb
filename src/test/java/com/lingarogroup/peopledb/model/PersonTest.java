package com.lingarogroup.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PersonTest {

    @Test
    public void testForEquality() {
        Person person1 = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person person2 = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        assertThat(person1).isEqualTo(person2);
    }

    @Test
    public void testForInequality() {
        Person person1 = new Person("John", "Smith", ZonedDateTime.now());
        Person person2 = new Person("Jane", "Smith", ZonedDateTime.now());
        assertThat(person1).isNotEqualTo(person2);
    }
}