package com.lingarogroup.peopledb.model;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

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
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstname='" + firstname + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dateOfBirth=" + dateOfBirth +
                ", id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(firstname, person.firstname)
                && Objects.equals(lastName, person.lastName)
                && Objects.equals(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")), person.dateOfBirth.withZoneSameInstant(ZoneId.of("+0")))
                && Objects.equals(id, person.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstname, lastName, dateOfBirth, id);
    }
}
