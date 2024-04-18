package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;

    @BeforeEach
    // it's better to throw SQLException than to catch it in test. Because if the exception will be thrown - the test will fail.
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/projects/JAVA/course/peopledb".replace("~", System.getProperty("user.home")));
    }

    @Test

    public void canSavePerson() {
        PeopleRepository repo = new PeopleRepository(connection);
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        PeopleRepository repo = new PeopleRepository(connection);
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedJohn = repo.save(john);
        Person jane = new Person("Jane", "Doe", ZonedDateTime.of(1985, 5, 20, 10, 30, 0, 0, ZoneId.of("-6")));
        Person savedJane = repo.save(jane);
        assertThat(savedJane.getId()).isNotEqualTo(savedJohn.getId());
    }
}
