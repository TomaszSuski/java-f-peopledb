package com.lingarogroup.peopledb.model;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Person implements Entity {
    private Long id;
    private String firstName;
    private String lastName;
    private ZonedDateTime dateOfBirth;
    private BigDecimal salary = BigDecimal.ZERO;

    public Person(String firstname, String lastName, ZonedDateTime dateOfBirth) {
        this.firstName = firstname;
        this.lastName = lastName;
        this.dateOfBirth = dateOfBirth;
    }

    public Person(long personId, String firstName, String lastName, ZonedDateTime dateOFBirth) {
        this(firstName, lastName, dateOFBirth); // calling the constructor with 3 parameters
        this.id = personId;
    }
    public Person(long personId, String firstName, String lastName, ZonedDateTime dateOFBirth, BigDecimal salary) {
        this(personId, firstName, lastName, dateOFBirth);   // calling the constructor with 4 parameters
        this.salary = salary;
    }

    @Override
    public Long getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public ZonedDateTime getDateOfBirth() {
        return dateOfBirth;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setDateOfBirth(ZonedDateTime dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstname='" + firstName + '\'' +
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
        return Objects.equals(firstName, person.firstName)
                && Objects.equals(lastName, person.lastName)
                && Objects.equals(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")), person.dateOfBirth.withZoneSameInstant(ZoneId.of("+0")))
                && Objects.equals(id, person.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstName, lastName, dateOfBirth, id);
    }
}
