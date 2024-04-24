package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.model.CrudOperation;
import com.lingarogroup.peopledb.annotation.SQL;
import com.lingarogroup.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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

//    @Override
//    protected String getFindByIdSql() {
//        return FIND_BY_ID_SQL;
//    }

//    @Override
//    protected String getFindAllSql() {
//        return FIND_ALL_SQL;
//    }
//
//    @Override
//    protected String getDeleteSql() {
//        return DELETE_PERSON_SQL;
//    }
//
//    @Override
//    protected String getCountSql() {
//        return COUNT_ALL_SQL;
//    }

        /**
     * This method is used to map the properties of a Person object to a PreparedStatement for saving the Person in the database.
     * The SQL query for this operation is provided by the SQL annotation.
     *
     * @param person The Person object whose properties should be mapped to the PreparedStatement.
     * @param ps The PreparedStatement to which the properties of the Person object should be mapped.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    @SQL(value = INSERT_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person person, PreparedStatement ps) throws SQLException {
        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        // we need to convert ZonedDateTime to LocalDateTime and then to Timestamp, standardising it to UTC to have the same value in the database
        ps.setTimestamp(3, convertDobToTimestamp(person.getDateOfBirth()));
    }

    /**
     * This method is used to map the properties of a Person object to a PreparedStatement for updating the Person in the database.
     * The SQL query for this operation is provided by the SQL annotation.
     *
     * @param person The Person object whose properties should be mapped to the PreparedStatement.
     * @param ps The PreparedStatement to which the properties of the Person object should be mapped.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    @SQL(value = UPDATE_PERSON_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person person, PreparedStatement ps) throws SQLException {
        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(person.getDateOfBirth()));
        ps.setBigDecimal(4, person.getSalary());
        ps.setLong(5, getIdByAnnotation(person));
    }

    /**
     * This method is used to extract a Person object from a ResultSet.
     * The SQL queries for finding by ID, finding all, counting, and deleting are provided by the SQL annotations.
     *
     * @param rs The ResultSet from which the Person object should be extracted.
     * @return The extracted Person object.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = COUNT_ALL_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_PERSON_SQL, operationType = CrudOperation.DELETE)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong(ID);
        String firstName = rs.getString(FIRST_NAME);
        String lastName = rs.getString(LAST_NAME);
        Timestamp dob = rs.getTimestamp(DOB);
        ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(SALARY);
        return new Person(personId, firstName, lastName, dateOFBirth, salary);
    }

    /**
     * This method is used to convert a ZonedDateTime to a Timestamp.
     * The ZonedDateTime is standardised to UTC to ensure that the same value is stored in the database regardless of the timezone.
     *
     * @param dateOfBirth The ZonedDateTime to be converted to a Timestamp.
     * @return The converted Timestamp.
     */
    private static Timestamp convertDobToTimestamp(ZonedDateTime dateOfBirth) {
        return Timestamp.valueOf(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
