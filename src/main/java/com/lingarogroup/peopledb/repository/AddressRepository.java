package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.annotation.SQL;
import com.lingarogroup.peopledb.exception.UnableToInitializeRepositoryException;
import com.lingarogroup.peopledb.model.Address;
import com.lingarogroup.peopledb.model.CrudOperation;
import com.lingarogroup.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address> {

    public static final String SAVE_ADDRESS_SQL = "INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTRY, COUNTY, REGION) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT * FROM ADDRESSES WHERE ID = ?";
    public static final String ID = "ID";
    public static final String STREET_ADDRESS = "STREET_ADDRESS";
    public static final String ADDRESS_2 = "ADDRESS2";
    public static final String CITY = "CITY";
    public static final String STATE = "STATE";
    public static final String POSTCODE = "POSTCODE";
    public static final String COUNTRY = "COUNTRY";
    public static final String COUNTY = "COUNTY";
    public static final String REGION = "REGION";

    public AddressRepository(Connection connection) throws UnableToInitializeRepositoryException {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = SAVE_ADDRESS_SQL)
    void mapForSave(Address entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getStreetAddress());
        ps.setString(2, entity.getAddress2());
        ps.setString(3, entity.getCity());
        ps.setString(4, entity.getState());
        ps.setString(5, entity.getPostcode());
        ps.setString(6, entity.getCountry());
        ps.setString(7, entity.getCounty());
        ps.setString(8, entity.getRegion().name());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }

    @Override
    @SQL(operationType = CrudOperation.FIND_BY_ID, value = FIND_BY_ID_SQL)
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long id = rs.getLong(ID);
        String streetAddress = rs.getString(STREET_ADDRESS);
        String address2 = rs.getString(ADDRESS_2);
        String city = rs.getString(CITY);
        String state = rs.getString(STATE);
        String postcode = rs.getString(POSTCODE);
        String country = rs.getString(COUNTRY);
        String county = rs.getString(COUNTY);
        Region region = Region.valueOf(rs.getString(REGION).toUpperCase());
        return new Address(id, streetAddress, address2, city, state, postcode, country, county, region);
    }
}
