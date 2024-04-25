package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.Address;
import com.lingarogroup.peopledb.model.Person;
import com.lingarogroup.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;
    private PeopleRepository repo;

    public void checkH2Version(Connection connection) {
    try {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT H2VERSION() AS VERSION FROM DUAL");
        if (resultSet.next()) {
            String version = resultSet.getString("VERSION");
            System.out.println("H2 Database Version: " + version);
        }
//        String dropTable = "DROP TABLE IF EXISTS PEOPLE";
//        PreparedStatement ps2 = connection.prepareStatement(dropTable);
//        ps2.executeUpdate();
//        String sql = """
//        CREATE TABLE IF NOT EXISTS PEOPLE (ID BIGINT AUTO_INCREMENT PRIMARY KEY, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), DOB TIMESTAMP, SALARY NUMERIC(10, 2));
//        ALTER TABLE PEOPLE ADD COLUMN EMAIL VARCHAR(255);
//        """;
//        PreparedStatement ps = connection.prepareStatement(sql);
//        ps.executeUpdate();

    } catch (SQLException e) {
        e.printStackTrace();
    }
}

    @BeforeEach
    // it's better to throw SQLException than to catch it in test. Because if the exception will be thrown - the test will fail.
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/projects/JAVA/course/peopledb".replace("~", System.getProperty("user.home")));
        connection.setAutoCommit(false);    // setting auto commit to false to avoid real data changes in the database
//        checkH2Version(connection);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    @Test

    public void canSavePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedJohn = repo.save(john);
        Person jane = new Person("Jane", "Doe", ZonedDateTime.of(1985, 5, 20, 10, 30, 0, 0, ZoneId.of("-6")));
        Person savedJane = repo.save(jane);
        assertThat(savedJane.getId()).isNotEqualTo(savedJohn.getId());
    }

    @Test
    public void canSavePersonWithAddress() {
        Person john = new Person("JohnZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address("123 Main St", "APT. 1A", "Anytown", "TX", "12345", "United States", "Foulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getHomeAddress().get().getId()).isGreaterThan(0);
    }

    @Test
    public void canFindPersonById() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        System.out.println(savedPerson);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address("123 Main St", "APT. 1A", "Anytown", "TX", "12345", "United States", "Foulton County", Region.WEST);
        john.setHomeAddress(address);
        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().get()).isEqualTo(savedPerson.getHomeAddress().get());
    }

    @Test
    public void  testPersonIdNotFound() {
        Optional<Person> person = repo.findById(-1L);
        assertThat(person).isEmpty();
    }

    @Test
    @Disabled
    public void canFindAllPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedJohn = repo.save(john);
        Person jane = new Person("Jane", "Doe", ZonedDateTime.of(1985, 5, 20, 10, 30, 0, 0, ZoneId.of("-6")));
        Person savedJane = repo.save(jane);
        Person tom = new Person("Tom", "Brown", ZonedDateTime.of(1990, 1, 1, 0, 0, 0, 0, ZoneId.of("-6")));
        Person savedTom = repo.save(tom);
        Person ann = new Person("Ann", "White", ZonedDateTime.of(1995, 6, 15, 12, 0, 0, 0, ZoneId.of("-6")));
        Person savedAnn = repo.save(ann);
        List<Person> people = repo.findAll();
        assertThat(people).containsExactlyInAnyOrder(savedJohn, savedJane, savedTom, savedAnn);
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("Jane", "Doe", ZonedDateTime.of(1985, 5, 20, 10, 30, 0, 0, ZoneId.of("-6"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(repo.findById(savedPerson.getId())).isNotEmpty();
        repo.delete(savedPerson);
        assertThat(repo.findById(savedPerson.getId())).isEmpty();
    }

    @Test
    public void canDeleteMultiple() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedJohn = repo.save(john);
        Person jane = new Person("Jane", "Doe", ZonedDateTime.of(1985, 5, 20, 10, 30, 0, 0, ZoneId.of("-6")));
        Person savedJane = repo.save(jane);
        assertThat(repo.findById(savedJohn.getId())).isNotEmpty();
        assertThat(repo.findById(savedJane.getId())).isNotEmpty();
        repo.delete(savedJohn, savedJane);
    }

    @Test
    public void canUpdatePerson() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        foundPerson.setSalary(new BigDecimal("73000.44"));
        repo.update(foundPerson);
        Person updatedPerson = repo.findById(savedPerson.getId()).get();
        assertThat(updatedPerson.getSalary()).isEqualByComparingTo("73000.44");
    }



    // separate code for check how to do some functionality
//    @Test
//    public void experiment() {
//        Person p1 = new Person(null, null, null);
//        p1.setId(10L);
//        Person p2 = new Person(null, null, null);
//        p2.setId(20L);
//        Person p3 = new Person(null, null, null);
//        p3.setId(30L);
//        Person p4 = new Person(null, null, null);
//        p4.setId(40L);
//        Person p5 = new Person(null, null, null);
//        p5.setId(50L);
//
//        Person[] people = Arrays.asList(p1, p2, p3, p4, p5).toArray(new Person[]{}); // convert List to Array
//
//        String ids = Arrays.stream(people)
//                .map(Person::getId)
//                .map(String::valueOf)
//                .reduce((s1, s2) -> s1 + "," + s2).orElse("");
//        System.out.println(ids);
//    }


    // separate code for loading data from csv file to db

//    @Test
//    public void loadData() throws IOException, SQLException {
//
//        List<String> columns = Files.lines(Path.of("/home/tomasz_suski/projects/JAVA/course/Employees/data/Hr5m.csv"))
//                .limit(1)
//                .map(str -> str.toLowerCase())
//                .map(line -> line.split(","))
//                .flatMap(Arrays::stream)
//                .toList();
//
//        int salaryIndex = columns.indexOf(("salary"));
//        int firstNameIndex = columns.indexOf("first name");
//        int lastNameIndex = columns.indexOf("last name");
//        int dobIndex = columns.indexOf("date of birth");
//        int tobIndex = columns.indexOf("time of birth");
//        int emailIndex = columns.indexOf("e mail");
//
//        Files.lines(Path.of("/home/tomasz_suski/projects/JAVA/course/Employees/data/Hr5m.csv"))
////                .parallel() // that won't work in here
//                .skip(1)
////                .limit(100)   // for testing purposes, for real data it was commented out
//                .map(line -> line.split(","))
//                .map(createPerson(firstNameIndex, lastNameIndex, dobIndex, tobIndex, salaryIndex, emailIndex))
//                .forEach(repo::save);
//        connection.commit();
//    }
//
//    private Function<String[], Person> createPerson(int firstNameIndex, int lastNameIndex, int dobIndex, int tobIndex, int salaryIndex, int emailIndex) {
//        return line -> {
//            Person person = new Person(line[firstNameIndex], line[lastNameIndex], getZonedDateTimeForPerson(line[dobIndex], line[tobIndex]));
//            person.setSalary(new BigDecimal(line[salaryIndex]));
//            person.setEmail(line[emailIndex]);
//            return person;
//        };
//    }
//
//    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy");
//    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm:ss a");
//    private ZonedDateTime getZonedDateTimeForPerson(String dob, String tob) {
//        LocalDate date = LocalDate.parse(dob, dateFormatter);
//        LocalTime time = LocalTime.parse(tob, timeFormatter);
//        return ZonedDateTime.of(date, time, ZoneId.of("+0"));
//    }
}
