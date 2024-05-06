package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToInitializeRepositoryException;
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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
    public static final String SPOUSE = "SPOUSE";
    public static final String EMAIL = "EMAIL";
    public static final String PARENT_ID = "PARENT_ID";

    public static final String INSERT_PERSON_SQL = """
        INSERT INTO PEOPLE
        (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, SECONDARY_ADDRESS, SPOUSE, PARENT_ID)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
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
//    public final String FIND_BY_ID_SQL = """
//            SELECT
//                p.ID, p.FIRST_NAME, p.LAST_NAME, p.DOB, p.SALARY, p.EMAIL, p.HOME_ADDRESS, p.SECONDARY_ADDRESS, p.SPOUSE,
//                home.ID as HOME_ID, home.STREET_ADDRESS as HOME_STREET_ADDRESS, home.ADDRESS2 as HOME_ADDRESS2, home.CITY as HOME_CITY, home.STATE as HOME_STATE, home.POSTCODE as HOME_POSTCODE, home.COUNTRY as HOME_COUNTRY, home.COUNTY as HOME_COUNTY, home.REGION as HOME_REGION,
//                secondary.ID as SECONDARY_ID, secondary.STREET_ADDRESS as SECONDARY_STREET_ADDRESS, secondary.ADDRESS2 as SECONDARY_ADDRESS2, secondary.CITY as SECONDARY_CITY, secondary.STATE as SECONDARY_STATE, secondary.POSTCODE as SECONDARY_POSTCODE, secondary.COUNTRY as SECONDARY_COUNTRY, secondary.COUNTY as SECONDARY_COUNTY, secondary.REGION as SECONDARY_REGION,
//                spouse.ID as SPOUSE_ID, spouse.FIRST_NAME as SPOUSE_FIRST_NAME, spouse.LAST_NAME as SPOUSE_LAST_NAME, spouse.DOB as SPOUSE_DOB, spouse.SALARY as SPOUSE_SALARY, spouse.EMAIL as SPOUSE_EMAIL, spouse.HOME_ADDRESS as SPOUSE_HOME_ADDRESS, spouse.SECONDARY_ADDRESS as SPOUSE_SECONDARY_ADDRESS, spouse.SPOUSE as SPOUSE_SPOUSE
//             FROM PEOPLE p
//             LEFT OUTER JOIN ADDRESSES AS home ON p.HOME_ADDRESS = home.ID
//             LEFT OUTER JOIN ADDRESSES AS secondary ON p.SECONDARY_ADDRESS = secondary.ID
//             LEFT OUTER JOIN PEOPLE AS spouse ON p.SPOUSE = spouse.ID
//             WHERE p.ID = ?
//            """;
    public final String FIND_BY_ID_SQL = """
            SELECT
                parent.ID AS PARENT_ID, parent.FIRST_NAME AS PARENT_FIRST_NAME, parent.LAST_NAME AS PARENT_LAST_NAME, parent.DOB AS PARENT_DOB, parent.SALARY AS PARENT_SALARY, parent.EMAIL AS PARENT_EMAIL,
                child.ID AS CHILD_ID, child.FIRST_NAME AS CHILD_FIRST_NAME, child.LAST_NAME AS CHILD_LAST_NAME, child.DOB AS CHILD_DOB, child.SALARY AS CHILD_SALARY, child.EMAIL AS CHILD_EMAIL,
                home.ID as HOME_ID, home.STREET_ADDRESS as HOME_STREET_ADDRESS, home.ADDRESS2 as HOME_ADDRESS2, home.CITY as HOME_CITY, home.STATE as HOME_STATE, home.POSTCODE as HOME_POSTCODE, home.COUNTRY as HOME_COUNTRY, home.COUNTY as HOME_COUNTY, home.REGION as HOME_REGION,
                secondary.ID as SECONDARY_ID, secondary.STREET_ADDRESS as SECONDARY_STREET_ADDRESS, secondary.ADDRESS2 as SECONDARY_ADDRESS2, secondary.CITY as SECONDARY_CITY, secondary.STATE as SECONDARY_STATE, secondary.POSTCODE as SECONDARY_POSTCODE, secondary.COUNTRY as SECONDARY_COUNTRY, secondary.COUNTY as SECONDARY_COUNTY, secondary.REGION as SECONDARY_REGION,
                spouse.ID as SPOUSE_ID, spouse.FIRST_NAME as SPOUSE_FIRST_NAME, spouse.LAST_NAME as SPOUSE_LAST_NAME, spouse.DOB as SPOUSE_DOB, spouse.SALARY as SPOUSE_SALARY, spouse.EMAIL as SPOUSE_EMAIL, spouse.HOME_ADDRESS as SPOUSE_HOME_ADDRESS, spouse.SECONDARY_ADDRESS as SPOUSE_SECONDARY_ADDRESS, spouse.SPOUSE as SPOUSE_SPOUSE
            FROM PEOPLE AS parent
            LEFT OUTER JOIN PEOPLE AS child ON parent.ID = child.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS home ON parent.HOME_ADDRESS = home.ID
            LEFT OUTER JOIN ADDRESSES AS secondary ON parent.SECONDARY_ADDRESS = secondary.ID
            LEFT OUTER JOIN PEOPLE AS spouse ON parent.SPOUSE = spouse.ID
            WHERE parent.ID = ?;
            """;

    public static final String FIND_ALL_SQL = """
            SELECT
                parent.ID AS PARENT_ID, parent.FIRST_NAME AS PARENT_FIRST_NAME, parent.LAST_NAME AS PARENT_LAST_NAME, parent.DOB AS PARENT_DOB, parent.SALARY AS PARENT_SALARY, parent.EMAIL AS PARENT_EMAIL,
                child.ID AS CHILD_ID, child.FIRST_NAME AS CHILD_FIRST_NAME, child.LAST_NAME AS CHILD_LAST_NAME, child.DOB AS CHILD_DOB, child.SALARY AS CHILD_SALARY, child.EMAIL AS CHILD_EMAIL,
                home.ID as HOME_ID, home.STREET_ADDRESS as HOME_STREET_ADDRESS, home.ADDRESS2 as HOME_ADDRESS2, home.CITY as HOME_CITY, home.STATE as HOME_STATE, home.POSTCODE as HOME_POSTCODE, home.COUNTRY as HOME_COUNTRY, home.COUNTY as HOME_COUNTY, home.REGION as HOME_REGION,
                secondary.ID as SECONDARY_ID, secondary.STREET_ADDRESS as SECONDARY_STREET_ADDRESS, secondary.ADDRESS2 as SECONDARY_ADDRESS2, secondary.CITY as SECONDARY_CITY, secondary.STATE as SECONDARY_STATE, secondary.POSTCODE as SECONDARY_POSTCODE, secondary.COUNTRY as SECONDARY_COUNTRY, secondary.COUNTY as SECONDARY_COUNTY, secondary.REGION as SECONDARY_REGION,
                spouse.ID as SPOUSE_ID, spouse.FIRST_NAME as SPOUSE_FIRST_NAME, spouse.LAST_NAME as SPOUSE_LAST_NAME, spouse.DOB as SPOUSE_DOB, spouse.SALARY as SPOUSE_SALARY, spouse.EMAIL as SPOUSE_EMAIL, spouse.HOME_ADDRESS as SPOUSE_HOME_ADDRESS, spouse.SECONDARY_ADDRESS as SPOUSE_SECONDARY_ADDRESS, spouse.SPOUSE as SPOUSE_SPOUSE, spouse.PARENT_ID as SPOUSE_PARENT_ID
            FROM PEOPLE AS parent
            LEFT OUTER JOIN PEOPLE AS child ON parent.ID = child.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS home ON parent.HOME_ADDRESS = home.ID
            LEFT OUTER JOIN ADDRESSES AS secondary ON parent.SECONDARY_ADDRESS = secondary.ID
            LEFT OUTER JOIN PEOPLE AS spouse ON parent.SPOUSE = spouse.ID
            """;
    public static final String COUNT_ALL_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    public static final String DELETE_PERSON_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    public static final String UPDATE_PERSON_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";

    public PeopleRepository(Connection connection) throws UnableToInitializeRepositoryException {
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
        associateAddressWithPerson(person.getHomeAddress(), ps, 6, "Unable to save Home Address");
        associateAddressWithPerson(person.getSecondaryAddress(), ps, 7, "Unable to save Secondary Address");
        associatePersonWithPerson(person.getSpouse(), ps, 8, "Unable to save Spouse");
        associatePersonWithPerson(person.getParent(), ps, 9, "Unable to save Parent");
    }

    /**
     * This method is called after a Person entity is saved to the database.
     * It iterates over the children of the saved Person entity and saves each child to the database.
     * The save operation is performed by calling the save method of this repository.
     *
     * @param entity The Person entity that has just been saved to the database.
     * @param id The ID of the saved Person entity in the database.
     */
    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().forEach(this::save);
    }

    private static void associatePersonWithPerson(Optional<Person> person, PreparedStatement ps, int spouseIdColumnIndex, String errorMessage) {
        person.ifPresentOrElse(
                spouse ->
                {
                    try {
                        ps.setLong(spouseIdColumnIndex, spouse.getId());
                    } catch (SQLException e) {
                        throw new UnableToSaveException(errorMessage);
                    }
                },
                () ->
                {
                    try {
                        ps.setNull(spouseIdColumnIndex, Types.BIGINT);
                    } catch (SQLException e) {
                        throw new UnableToSaveException(errorMessage + " for null spouse");
                    }
                }
        );
    }

    private void associateAddressWithPerson(Optional<Address> address, PreparedStatement ps, int addressIdColumnIndex, String errorMessage) {
        address.ifPresentOrElse(
                a ->
                        saveAddress(ps, addressIdColumnIndex, a, errorMessage),
                () ->
                        saveNullAddress(ps, addressIdColumnIndex, errorMessage + " for null address")
        );
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
        Person parent = null;
        do {
            Person extractedParent = extractPerson(rs, "PARENT_");
            if (parent == null) {
                parent = extractedParent;

            }
            if (!extractedParent.equals(parent)) {
                rs.previous();
                break;
            }
            Address homeAddress = extractAddress(rs, "HOME_");
            Address secondaryAddress = extractAddress(rs, "SECONDARY_");
            Long spouseId = (Long) rs.getObject("SPOUSE_"+ID);
            Person spouse = extractSpouse(rs, spouseId);
            parent.setHomeAddress(homeAddress);
            parent.setSecondaryAddress(secondaryAddress);
            parent.setSpouse(spouse);
            Person child = extractPerson(rs, "CHILD_");
            if (child != null) {
                parent.addChild(child);
            }
        } while (rs.next());
        return parent;
    }

    private Person extractPerson(ResultSet rs, String aliasPrefix) throws SQLException {
        long personId = rs.getLong(aliasPrefix + ID);
        if (personId == 0) return null;
        String firstName = rs.getString(aliasPrefix + FIRST_NAME);
        String lastName = rs.getString(aliasPrefix + LAST_NAME);
        Timestamp dob = rs.getTimestamp(aliasPrefix + DOB);
        ZonedDateTime dateOFBirth = dob.toLocalDateTime().atZone(ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(aliasPrefix + SALARY);
        return new Person(personId, firstName, lastName, dateOFBirth, salary);
    }
    private Set<Person> associateChildren(long personId) throws SQLException {
        Set<Person> children = new HashSet<>();
        ResultSet crs = this.findChildrenByParentId(personId);
        while (crs.next()) {
            children.add(extractEntityFromResultSet(crs));
        }
        return children;
    }

    private ResultSet findChildrenByParentId(long personId) {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM PEOPLE WHERE PARENT_ID = ?");
            ps.setLong(1, personId);
            return ps.executeQuery();
        } catch (SQLException e) {
            throw new UnableToLoadException("Unable to find children for person with ID: " + personId);
        }
    }

    private Person extractSpouse(ResultSet rs, Long spouseId) throws SQLException {
        Person spouse = null;
        if (spouseId != null) {
            spouse = new Person(spouseId, rs.getString("SPOUSE_"+FIRST_NAME), rs.getString("SPOUSE_"+LAST_NAME), rs.getTimestamp("SPOUSE_"+DOB).toLocalDateTime().atZone(ZoneId.of("+0")), rs.getBigDecimal("SPOUSE_"+SALARY));
            spouse.setEmail(rs.getString("SPOUSE_"+EMAIL));
            Address spouseHomeAddress = extractAddress(rs, "HOME_");
            Address spouseSecondaryAddress = extractAddress(rs, "SECONDARY_");
            spouse.setHomeAddress(spouseHomeAddress);
            spouse.setSecondaryAddress(spouseSecondaryAddress);
        }
        return spouse;
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
