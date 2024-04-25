package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.annotation.SQL;
import com.lingarogroup.peopledb.model.Address;
import com.lingarogroup.peopledb.model.CrudOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address> {

    public static final String SAVE_ADDRESS_SQL = "INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTRY, COUNTY, REGION) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    public AddressRepository(Connection connection) {
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
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        return null;
    }
}
