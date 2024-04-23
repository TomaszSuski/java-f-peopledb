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

public class PeopleRepository extends CRUDRepository<Person> {

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

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    public String getSaveSql() {
        return INSERT_PERSON_SQL;
    }

    @Override
    void mapForSave(Person person, PreparedStatement ps) throws SQLException {
        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        // we need to convert ZonedDateTime to LocalDateTime and then to Timestamp, standardising it to UTC to have the same value in the database
        ps.setTimestamp(3, convertDobToTimestamp(person.getDateOfBirth()));
    }

    @Override
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong(ID);
        String firstName = rs.getString(FIRST_NAME);
        String lastName = rs.getString(LAST_NAME);
        Timestamp dob = rs.getTimestamp(DOB);
        ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(SALARY);
        return new Person(personId, firstName, lastName, dateOFBirth, salary);
    }

    @Override
    String getFindByIdSql() {
        return FIND_BY_ID_SQL;
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
