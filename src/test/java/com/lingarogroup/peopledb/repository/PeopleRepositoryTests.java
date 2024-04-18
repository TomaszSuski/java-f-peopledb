package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Person;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {
    @Test
    public void canSavePerson() {
        PeopleRepository repo = new PeopleRepository();
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }
}
