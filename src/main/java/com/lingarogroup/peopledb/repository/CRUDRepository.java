package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Entity;
import com.lingarogroup.peopledb.model.Person;

import java.sql.*;
import java.util.Optional;

public abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            // prepare statement to avoid SQL injection, it has the ability to return auto-generated keys
            PreparedStatement ps = connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int rowsAffected = ps.executeUpdate();
            // getGeneratedKeys returns the result set containing the auto-generated keys
            ResultSet rs = ps.getGeneratedKeys();
            // to retrieve the auto-generated key we need to iterate over the result set
            while (rs.next()) {
                // getLong(1) returns the value of the first column
                // there is also version with column name
                long id = rs.getLong(1);
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Rows affected: %d%n", rowsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Unable to save person: " + entity);
        }
        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;
        try {
            PreparedStatement ps = connection.prepareStatement(getFindByIdSql());
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to find entity with id: " + id);
        }
        return Optional.ofNullable(entity);
    }

    // existing code...

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
    abstract String getFindByIdSql();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();
}
