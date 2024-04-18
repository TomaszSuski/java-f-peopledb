package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Person;

import java.sql.Connection;

public class PeopleRepository {

    private final Connection connection;

    public PeopleRepository(Connection connection) {

        this.connection = connection;
    }

    public Person save(Person person) {
        // Save the person to the database
        return person;
    }
}
