package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;

public void checkH2Version(Connection connection) {
    try {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT H2VERSION() AS VERSION FROM DUAL");
        if (resultSet.next()) {
            String version = resultSet.getString("VERSION");
            System.out.println("H2 Database Version: " + version);
        }
        String sql = "CREATE TABLE IF NOT EXISTS PEOPLE (ID BIGINT AUTO_INCREMENT PRIMARY KEY, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), DOB TIMESTAMP, SALARY DECIMAL(10, 2)); SELECT * FROM PEOPLE;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeUpdate();

    } catch (SQLException e) {
        e.printStackTrace();
    }
}

    @BeforeEach
    // it's better to throw SQLException than to catch it in test. Because if the exception will be thrown - the test will fail.
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/projects/JAVA/course/peopledb".replace("~", System.getProperty("user.home")));
        connection.setAutoCommit(false);    // setting auto commit to false to avoid real data changes in the database
        checkH2Version(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
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
