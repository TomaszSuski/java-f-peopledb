package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Address;
import com.lingarogroup.peopledb.model.CrudOperation;
import com.lingarogroup.peopledb.annotation.SQL;
import com.lingarogroup.peopledb.model.Person;
import com.lingarogroup.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class PeopleRepository extends CRUDRepository<Person> {
    // The AddressRepository is used to save the home address of a Person object.
    // but it's a tight coupling, we should use a service to handle the address
    private AddressRepository addressRepository;

    public static final String ID = "ID";
    public static final String FIRST_NAME = "FIRST_NAME";
    public static final String LAST_NAME = "LAST_NAME";
    public static final String DOB = "DOB";
    public static final String SALARY = "SALARY";
    public static final String HOME_ADDRESS = "HOME_ADDRESS";
    public static final String SECONDARY_ADDRESS = "SECONDARY_ADDRESS";

    public static final String INSERT_PERSON_SQL = """
        INSERT INTO PEOPLE
        (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, SECONDARY_ADDRESS, SPOUSE)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """;
    //    public static String findByIdSql = String.format("""
//        SELECT
//         home.ID as HOME_ID, p.*,
//            home.STREET_ADDRESS as HOME_STREET_ADDRESS, home.ADDRESS2 as HOME_ADDRESS2, home.CITY as HOME_CITY, home.STATE as HOME_STATE, home.POSTCODE as HOME_POSTCODE, home.COUNTRY as HOME_COUNTRY, home.COUNTY as HOME_COUNTY, home.REGION as HOME_REGION,
//            secondary.ID as SECONDARY_ID, secondary.STREET_ADDRESS as SECONDARY_STREET_ADDRESS, secondary.ADDRESS2 as SECONDARY_ADDRESS2, secondary.CITY as SECONDARY_CITY, secondary.STATE as SECONDARY_STATE, secondary.POSTCODE as SECONDARY_POSTCODE, secondary.COUNTRY as SECONDARY_COUNTRY, secondary.COUNTY as SECONDARY_COUNTY, secondary.REGION as SECONDARY_REGION
//         FROM PEOPLE p
//         LEFT OUTER JOIN ADDRESSES AS home ON p.HOME_ADDRESS = home.ID
//         LEFT OUTER JOIN ADDRESSES AS secondary ON p.SECONDARY_ADDRESS = secondary.ID
//         WHERE p.ID = ?
//        """);
    public final String FIND_BY_ID_SQL = """
            SELECT
                p.ID, p.FIRST_NAME, p.LAST_NAME, p.DOB, p.SALARY, p.EMAIL, p.HOME_ADDRESS, p.SECONDARY_ADDRESS, p.SPOUSE,
                home.ID as HOME_ID, home.STREET_ADDRESS as HOME_STREET_ADDRESS, home.ADDRESS2 as HOME_ADDRESS2, home.CITY as HOME_CITY, home.STATE as HOME_STATE, home.POSTCODE as HOME_POSTCODE, home.COUNTRY as HOME_COUNTRY, home.COUNTY as HOME_COUNTY, home.REGION as HOME_REGION,
                secondary.ID as SECONDARY_ID, secondary.STREET_ADDRESS as SECONDARY_STREET_ADDRESS, secondary.ADDRESS2 as SECONDARY_ADDRESS2, secondary.CITY as SECONDARY_CITY, secondary.STATE as SECONDARY_STATE, secondary.POSTCODE as SECONDARY_POSTCODE, secondary.COUNTRY as SECONDARY_COUNTRY, secondary.COUNTY as SECONDARY_COUNTY, secondary.REGION as SECONDARY_REGION,
                spouse.ID as SPOUSE_ID, spouse.FIRST_NAME as SPOUSE_FIRST_NAME, spouse.LAST_NAME as SPOUSE_LAST_NAME, spouse.DOB as SPOUSE_DOB, spouse.SALARY as SPOUSE_SALARY, spouse.EMAIL as SPOUSE_EMAIL, spouse.HOME_ADDRESS as SPOUSE_HOME_ADDRESS, spouse.SECONDARY_ADDRESS as SPOUSE_SECONDARY_ADDRESS, spouse.SPOUSE as SPOUSE_SPOUSE
             FROM PEOPLE p
             LEFT OUTER JOIN ADDRESSES AS home ON p.HOME_ADDRESS = home.ID
             LEFT OUTER JOIN ADDRESSES AS secondary ON p.SECONDARY_ADDRESS = secondary.ID
             LEFT OUTER JOIN PEOPLE AS spouse ON p.SPOUSE = spouse.ID
             WHERE p.ID = ?
            """;
    public static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";
    public static final String COUNT_ALL_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    public static final String DELETE_PERSON_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    public static final String UPDATE_PERSON_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

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
        ps.setBigDecimal(4, person.getSalary());
        ps.setString(5, person.getEmail());
        person.getHomeAddress().ifPresentOrElse(
                address ->
                        saveAddress(ps, 6, address, "Unable to save Home Address"),
                () ->
                        saveNullAddress(ps, 6, "Unable to save null address")
        );
        person.getSecondaryAddress().ifPresentOrElse(
                address ->
                        saveAddress(ps, 7, address, "Unable to save Secondary Address"),
                () ->
                        saveNullAddress(ps, 7, "Unable to save null secondary address")
        );
        person.getSpouse().ifPresentOrElse(
                spouse ->
                {
                    try {
                        ps.setLong(8, spouse.getId());
                    } catch (SQLException e) {
                        throw new UnableToSaveException("Unable to save Spouse");
                    }
                },
                () ->
                {
                    try {
                        ps.setNull(8, Types.BIGINT);
                    } catch (SQLException e) {
                        throw new UnableToSaveException("Unable to save null spouse");
                    }
                }
        );
        // explicit null check
//        if (person.getHomeAddress() != null) {
//            savedAdress = addressRepository.save(person.getHomeAddress());
//            ps.setLong(6, savedAdress.getId());
//        } else {
//            ps.setNull(6, Types.BIGINT);
//        }

        // using optional to avoid explicit null check

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

        Address homeAddress = extractAddress(rs, "HOME_");
        Address secondaryAddress = extractAddress(rs, "SECONDARY_");
        Long spouseId = (Long) rs.getObject("SPOUSE_ID");
        Person spouse = null;
        if (spouseId != null) {
            spouse = new Person(spouseId, rs.getString("SPOUSE_FIRST_NAME"), rs.getString("SPOUSE_LAST_NAME"), rs.getTimestamp("SPOUSE_DOB").toLocalDateTime().atZone(ZoneId.of("+0")), rs.getBigDecimal("SPOUSE_SALARY"));
            spouse.setEmail(rs.getString("SPOUSE_EMAIL"));
            Address spouseHomeAddress = extractAddress(rs, "HOME_");
            Address spouseSecondaryAddress = extractAddress(rs, "SECONDARY_");
            spouse.setHomeAddress(spouseHomeAddress);
            spouse.setSecondaryAddress(spouseSecondaryAddress);
        }

        Person person = new Person(personId, firstName, lastName, dateOFBirth, salary);
        person.setHomeAddress(homeAddress);
        person.setSecondaryAddress(secondaryAddress);
        person.setSpouse(spouse);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        String addressId = aliasPrefix + AddressRepository.ID;
        if (rs.getObject(addressId) == null) return null;
        long id = rs.getLong(addressId);
        String streetAddress = rs.getString(aliasPrefix + AddressRepository.STREET_ADDRESS);
        String address2 = rs.getString(aliasPrefix + AddressRepository.ADDRESS_2);
        String city = rs.getString(aliasPrefix + AddressRepository.CITY);
        String state = rs.getString(aliasPrefix + AddressRepository.STATE);
        String postcode = rs.getString(aliasPrefix + AddressRepository.POSTCODE);
        String country = rs.getString(aliasPrefix + AddressRepository.COUNTRY);
        String county = rs.getString(aliasPrefix + AddressRepository.COUNTY);
        Region region = Region.valueOf(rs.getString(aliasPrefix + AddressRepository.REGION).toUpperCase());
        return new Address(id, streetAddress, address2, city, state, postcode, country, county, region);
    }

    private void saveAddress(PreparedStatement ps, int addressIdColumnIndex, Address address, String exceptionMessage) throws UnableToSaveException {
            try {
                Address savedAddress = addressRepository.save(address);
                ps.setLong(addressIdColumnIndex, savedAddress.getId());
            } catch (SQLException e) {
                throw new UnableToSaveException(exceptionMessage);
            }
    }

    private void saveNullAddress(PreparedStatement ps, int addressIdColumnIndex, String exceptionMessage) {
        try {
            ps.setNull(addressIdColumnIndex, Types.BIGINT);
        } catch (SQLException e) {
            throw new UnableToSaveException(exceptionMessage);
        }
    }

    /**
     * This method is used to convert a ZonedDateTime to a Timestamp.
     * The ZonedDateTime is standardised to UTC to ensure that the same value is stored in the database regardless of the timezone.
     *
     * @param dateOfBirth The ZonedDateTime to be converted to a Timestamp.
     * @return The converted Timestamp.
     */
    private Timestamp convertDobToTimestamp(ZonedDateTime dateOfBirth) {
        return Timestamp.valueOf(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
