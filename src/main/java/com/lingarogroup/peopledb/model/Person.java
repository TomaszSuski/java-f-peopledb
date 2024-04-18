package com.lingarogroup.peopledb.model;

import java.time.ZonedDateTime;

public class Person {
    private final String firstname;
    private final String lastName;
    private final ZonedDateTime dateOfBirth;
    private Long id;

    public Person(String firstname, String lastName, ZonedDateTime dateOfBirth) {
        this.firstname = firstname;
        this.lastName = lastName;
        this.dateOfBirth = dateOfBirth;
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastName() {
        return lastName;
    }

    public ZonedDateTime getDateOfBirth() {
        return dateOfBirth;
    }

    public Long getId() {
        return 1L;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
