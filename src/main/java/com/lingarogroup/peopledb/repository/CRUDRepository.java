package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Entity;
import com.lingarogroup.peopledb.model.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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

    public Optional<T> findById(Long id) throws UnableToLoadException {
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

    public List<T> findAll() throws UnableToLoadException {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(getFindAllSql());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                T entity = extractEntityFromResultSet(rs);
                entities.add(entity);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to find entities");
        }
        return entities;
    }

 public void update(T entity) {
    try {
        PreparedStatement ps = connection.prepareStatement(getUpdateSql());
        mapForUpdate(entity, ps);
        int rowsAffected = ps.executeUpdate();
        System.out.printf("Rows affected: %d%n", rowsAffected);
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
}

    public void delete(T entity) {
    try {
        PreparedStatement ps = connection.prepareStatement(getDeleteSql());
        ps.setLong(1, entity.getId());
        int affectedRecords = ps.executeUpdate();
        System.out.println("Affected records with delete: " + affectedRecords);
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
}

public void delete(T... entities) {
    try {
        PreparedStatement ps = connection.prepareStatement(getDeleteSql());
        for (T entity : entities) {
            ps.setLong(1, entity.getId());
            ps.addBatch();
        }
        int[] affectedRecords = ps.executeBatch();
        System.out.println("Affected records with delete: " + affectedRecords.length);
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
}

abstract String getDeleteSql();

abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
abstract String getUpdateSql();

    abstract String getFindAllSql();

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    abstract String getFindByIdSql();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();
}