package com.lingarogroup.peopledb.repository;

import com.lingarogroup.peopledb.annotation.CrudOperation;
import com.lingarogroup.peopledb.annotation.SQL;
import com.lingarogroup.peopledb.exception.UnableToDeleteException;
import com.lingarogroup.peopledb.exception.UnableToLoadException;
import com.lingarogroup.peopledb.exception.UnableToSaveException;
import com.lingarogroup.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    /**
     * This method is used to save an entity to the database.
     * It prepares a SQL statement to avoid SQL injection and has the ability to return auto-generated keys.
     * The entity's fields are mapped to the PreparedStatement's parameters by calling the mapForSave method.
     * The SQL statement is executed and the number of affected rows is printed.
     * The auto-generated key is retrieved from the ResultSet and set as the ID of the entity.
     * If a SQLException occurs, an UnableToSaveException is thrown.
     *
     * @param entity The entity to be saved.
     * @return The saved entity with the auto-generated ID.
     * @throws UnableToSaveException If a SQLException occurs.
     */
    public T save(T entity) throws UnableToSaveException {
        try {
            // prepare statement to avoid SQL injection, it has the ability to return auto-generated keys
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
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

    /**
     * This method is used to find an entity by its ID.
     * It prepares a SQL statement and sets the ID as the parameter.
     * The SQL statement is executed and the entity is extracted from the ResultSet.
     * If a SQLException occurs, an UnableToLoadException is thrown.
     *
     * @param id The ID of the entity to be found.
     * @return An Optional that contains the found entity, or an empty Optional if the entity was not found.
     * @throws UnableToLoadException If a SQLException occurs.
     */
    public Optional<T> findById(Long id) throws UnableToLoadException {
        T entity = null;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
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

    /**
     * This method is used to find all entities.
     * It prepares a SQL statement and executes it.
     * The entities are extracted from the ResultSet and added to a list.
     * If a SQLException occurs, an UnableToLoadException is thrown.
     *
     * @return A list of all entities.
     * @throws UnableToLoadException If a SQLException occurs.
     */
    public List<T> findAll() throws UnableToLoadException {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql));
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

    /**
     * This method is used to update an entity in the database.
     * It prepares a SQL statement and maps the entity's fields to the PreparedStatement's parameters by calling the mapForUpdate method.
     * The SQL statement is executed and the number of affected rows is printed.
     * If a SQLException occurs, an UnableToSaveException is thrown.
     *
     * @param entity The entity to be updated.
     * @throws UnableToSaveException If a SQLException occurs.
     */
     public void update(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateSql));
            mapForUpdate(entity, ps);
            int rowsAffected = ps.executeUpdate();
            System.out.printf("Rows affected: %d%n", rowsAffected);
        } catch (SQLException e) {
            throw new UnableToSaveException("Unable to update entity");
        }
    }

    /**
     * This method is used to delete an entity from the database.
     * It prepares a SQL statement and sets the ID of the entity as the parameter.
     * The SQL statement is executed and the number of affected records is printed.
     * If a SQLException occurs, an UnableToDeleteException is thrown.
     *
     * @param entity The entity to be deleted.
     * @throws UnableToDeleteException If a SQLException occurs.
     */
    public void delete(T entity) throws UnableToDeleteException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE, this::getDeleteSql));
            ps.setLong(1, entity.getId());
            int affectedRecords = ps.executeUpdate();
            System.out.println("Affected records with delete: " + affectedRecords);
        } catch (SQLException e) {
            throw new UnableToDeleteException("Unable to delete entity");
        }
    }

    /**
     * This method is used to delete multiple entities from the database.
     * It uses a batch operation to execute the delete SQL statement for each entity.
     * The delete SQL statement is obtained by calling the getDeleteSql() method.
     * The ID of each entity is set as the parameter of the delete SQL statement.
     *
     * @param entities The entities to be deleted.
     * @throws RuntimeException If a database access error occurs or this method is called on a closed PreparedStatement.
     */
    public void delete(T... entities) throws UnableToDeleteException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE, this::getDeleteSql));
            for (T entity : entities) {
                ps.setLong(1, entity.getId());
                ps.addBatch();
            }
            int[] affectedRecords = ps.executeBatch();
            System.out.println("Affected records with delete: " + affectedRecords.length);
        } catch (SQLException e) {
            throw new UnableToDeleteException("Unable to delete entities");
        }
    }

    /**
     * This method is used to count the total number of entities in the database.
     * It executes the SQL statement obtained by calling the getCountSql() method.
     * The result of the SQL statement is a single row with a single column that contains the count of entities.
     * This count is extracted from the ResultSet and returned by this method.
     *
     * @return The total number of entities in the database.
     * @throws UnableToLoadException If a database access error occurs or this method is called on a closed PreparedStatement.
     */
    public long count() throws UnableToLoadException {
        long count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                count = rs.getLong("COUNT");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToLoadException("Unable to count entities");
        }
        return count;
    }

    /**
     * This method is used to retrieve the SQL query associated with a specific CRUD operation in the class.
     * It does this by looking for a method with the given CRUD operation type and retrieving the value of its SQL annotation.
     * If no such method is found, or if the method does not have a SQL annotation, it uses the provided Supplier to get a default SQL query.
     *
     * @param operationType The type of the CRUD operation whose SQL annotation value should be retrieved.
     * @param sqlGetter A Supplier that provides a default SQL query if the method does not exist or does not have a SQL annotation.
     * @return The SQL query associated with the method, or the default SQL query if the method does not exist or does not have a SQL annotation.
     *
     * Here is a step-by-step explanation of what this method does:
     * 1. It uses reflection to get all declared methods of the current class.
     * 2. It filters out methods that do not have the SQL annotation.
     * 3. It maps each method to its SQL annotation.
     * 4. It filters out SQL annotations that do not match the provided CRUD operation type.
     * 5. It maps each SQL annotation to its value (i.e., the SQL query).
     * 6. It tries to get the first SQL query from the stream. If no SQL query is found, it uses the provided Supplier to get a default SQL query.
     */
    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        return Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(SQL.class))
                .map(method -> method.getAnnotation(SQL.class))
                .filter(sql -> sql.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    /**
     * This method should return a SQL statement for counting the total number of entities in the database.
     * The returned SQL statement should not require any parameters.
     *
     * @return A SQL statement for counting the total number of entities in the database.
     */
    protected String getCountSql() {return "";};

    /**
     * This method should return a SQL statement for deleting an entity.
     * The returned SQL statement should have exactly one parameter which is the ID of the entity.
     *
     * @return A SQL statement for deleting an entity.
     */
    protected String getDeleteSql() {return "";};

    /**
     * This method should return a SQL statement for updating an entity.
     * The returned SQL statement should have parameters that correspond to the entity's fields.
     *
     * @return A SQL statement for updating an entity.
     */
    protected String getUpdateSql() {return "";}

    /**
     * This method should return a SQL statement for retrieving all entities.
     * The returned SQL statement should not require any parameters.
     *
     * @return A SQL statement for retrieving all entities.
     */
    protected String getFindAllSql() {return "";};

    /**
     * This method should return a SQL statement for finding an entity by its ID.
     * The returned SQL statement must contain exactly one parameter which is the ID of the entity.
     *
     * @return A SQL statement for finding an entity by its ID.
     */
    protected String getFindByIdSql() {return "";};

    /**
     * This method should return a SQL statement for saving an entity.
     * The returned SQL statement should have parameters that correspond to the entity's fields.
     *
     * @return A SQL statement for saving an entity.
     */
   protected String getSaveSql() {return "";};

    /**
     * This method is used to map the entity's fields to the PreparedStatement's parameters for the save operation.
     * The implementation of this method will vary depending on the specific entity type.
     *
     * @param entity The entity that is being saved.
     * @param ps The PreparedStatement object that is used for the save operation.
     * @throws SQLException If a database access error occurs or this method is called on a closed PreparedStatement.
     */
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    /**
     * This method is used to map the entity's fields to the PreparedStatement's parameters for the update operation.
     * The implementation of this method will vary depending on the specific entity type.
     *
     * @param entity The entity that is being updated.
     * @param ps The PreparedStatement object that is used for the update operation.
     * @throws SQLException If a database access error occurs or this method is called on a closed PreparedStatement.
     */
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;


    /**
     * This method is used to extract an entity from the ResultSet object.
     * The implementation of this method will vary depending on the specific entity type.
     *
     * @param rs The ResultSet object that contains the data of the entity.
     * @return The entity that was extracted from the ResultSet.
     * @throws SQLException If a database access error occurs or this method is called on a closed ResultSet.
     */
    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
}
