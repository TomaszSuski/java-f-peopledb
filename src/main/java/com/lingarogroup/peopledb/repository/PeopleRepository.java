package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Person;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public class PeopleRepository {

    public static final String INSERT_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB FROM PEOPLE WHERE ID = ?";
    public static final String ID = "ID";
    public static final String FIRST_NAME = "FIRST_NAME";
    public static final String LAST_NAME = "LAST_NAME";
    public static final String DOB = "DOB";
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB FROM PEOPLE";
    public static final String COUNT_ALL_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private final Connection connection;

    public PeopleRepository(Connection connection) {

        this.connection = connection;
    }

    public Person save(Person person) throws UnableToSaveException {
        // Save the person to the database
        // one of possibilities to create sql statement
//        String sql = String.format("INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES ('%s', '%s', %s)", person.getFirstname(), person.getLastName(), person.getDateOfBirth());
        // another approach with question marks as in constant INSERT_PERSON_SQL
        // the prepared statement is used to avoid SQL injection
        try {
            // prepare statement to avoid SQL injection, it has the ability to return auto-generated keys
            PreparedStatement ps = connection.prepareStatement(INSERT_PERSON_SQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, person.getFirstname());
            ps.setString(2, person.getLastName());
            // we need to convert ZonedDateTime to LocalDateTime and then to Timestamp, standardising it to UTC to have the same value in the database
            ps.setTimestamp(3, java.sql.Timestamp.valueOf(person.getDateOfBirth().withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime()));
            // executeUpdate returns the number of rows affected
            int rowsAffected = ps.executeUpdate();
            // getGeneratedKeys returns the result set containing the auto-generated keys
            ResultSet rs = ps.getGeneratedKeys();
            // to retrieve the auto-generated key we need to iterate over the result set
            while (rs.next()) {
                // getLong(1) returns the value of the first column
                // there is also version with column name
                long id = rs.getLong(1);
                person.setId(id);
                System.out.println(person);
            }
            System.out.printf("Rows affected: %d%n", rowsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Unable to save person: " + person);
        }
        return person;
    }

    public Optional<Person> findById(Long id) {
        Person person = null;
        try {
            PreparedStatement ps = connection.prepareStatement(FIND_BY_ID_SQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long personId = rs.getLong(ID);
                String firstName = rs.getString(FIRST_NAME);
                String lastName = rs.getString(LAST_NAME);
                Timestamp dob = rs.getTimestamp(DOB);
                ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
                person = new Person(firstName, lastName, dateOFBirth);
                person.setId(personId);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to find person with id: " + id);
        }
        return Optional.ofNullable(person);
    }

    public List<Person> findAll() {
        List<Person> people = new java.util.ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(FIND_ALL_SQL);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long personId = rs.getLong(ID);
                String firstName = rs.getString(FIRST_NAME);
                String lastName = rs.getString(LAST_NAME);
                Timestamp dob = rs.getTimestamp(DOB);
                ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
                Person person = new Person(firstName, lastName, dateOFBirth);
                person.setId(personId);
                people.add(person);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to find people");
        }
        return people;
    }

    public long count() {
        long count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(COUNT_ALL_SQL);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                count = rs.getLong("COUNT");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to count people");
        }
        return count;
    }
}
