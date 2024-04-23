package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public class PeopleRepository {

    public static final String ID = "ID";
    public static final String FIRST_NAME = "FIRST_NAME";
    public static final String LAST_NAME = "LAST_NAME";
    public static final String DOB = "DOB";
    public static final String SALARY = "SALARY";
    public static final String INSERT_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    public static final String COUNT_ALL_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    public static final String DELETE_PERSON_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    public static final String UPDATE_PERSON_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";
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
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            // we need to convert ZonedDateTime to LocalDateTime and then to Timestamp, standardising it to UTC to have the same value in the database
            ps.setTimestamp(3, convertDobToTimestamp(person.getDateOfBirth()));
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
                person =  extractPersonFromResultSet(rs);
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
                Person person = extractPersonFromResultSet(rs);
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

    public void delete(Person person) {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(DELETE_PERSON_SQL);
            ps.setLong(1, person.getId());
            int affectedRecords = ps.executeUpdate();
            System.out.println("Affected records with delete: " + affectedRecords);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(Person... people) {
        // the quick and easy way to delete multiple people
//        for (Person person : people) {
//            delete(person);
//        }

        // but for learning db purposes we can use batch processing
        // it's more efficient to use batch processing for multiple deletes
        // because it sends multiple queries in one go
        try {
            PreparedStatement ps = connection.prepareStatement(DELETE_PERSON_SQL);
            for (Person person : people) {
                ps.setLong(1, person.getId());
                ps.addBatch();
            }
            int[] affectedRecords = ps.executeBatch();
            System.out.println("Affected records with delete: " + affectedRecords.length);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // another example, now using createStatement
        // using one statement for multiple deletes with multiple ids
        // BUT it's not recommended to use this approach because of SQL injection
//        String ids = Arrays.stream(people)
//                .map(Person::getId)
//                .map(String::valueOf)
//                .reduce((s1, s2) -> s1 + "," + s2).orElse("");
//        try {
//            Statement statement = connection.createStatement();
//            int affectedRecords = statement.executeUpdate("DELETE FROM PEOPLE WHERE ID IN (:ids)".replace(":ids", ids));// :ids is a named parameter
//            System.out.println("Affected records with delete: " + affectedRecords);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void update(Person person) {
        // update the person in the database
        // one of possibilities to create sql statement
        try {
            PreparedStatement ps = connection.prepareStatement(UPDATE_PERSON_SQL);
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, convertDobToTimestamp(person.getDateOfBirth()));
            ps.setBigDecimal(4, person.getSalary());
            ps.setLong(5, person.getId());
            int rowsAffected = ps.executeUpdate();
            System.out.printf("Rows affected: %d%n", rowsAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Person extractPersonFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong(ID);
        String firstName = rs.getString(FIRST_NAME);
        String lastName = rs.getString(LAST_NAME);
        Timestamp dob = rs.getTimestamp(DOB);
        ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(SALARY);
        return new Person(personId, firstName, lastName, dateOFBirth, salary);
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dateOfBirth) {
        return Timestamp.valueOf(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
