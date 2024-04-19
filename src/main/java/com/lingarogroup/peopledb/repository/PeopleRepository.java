package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Person;

import java.sql.*;
import java.time.ZoneId;

public class PeopleRepository {

    private final Connection connection;

    public PeopleRepository(Connection connection) {

        this.connection = connection;
    }

    public Person save(Person person) {
        // Save the person to the database
        // one of possibilities to create sql statement
//        String sql = String.format("INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES ('%s', '%s', %s)", person.getFirstname(), person.getLastName(), person.getDateOfBirth());
        // another approach with question marks
        String sql = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
        try {
            // prepare statement to avoid SQL injection, it has the ability to return auto-generated keys
            PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
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
            }
            System.out.printf("Rows affected: %d%n", rowsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return person;
    }
}
