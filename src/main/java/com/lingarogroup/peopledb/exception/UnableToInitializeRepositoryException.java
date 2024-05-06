package com.lingarogroup.peopledb.exception;

import java.sql.SQLException;

public class UnableToInitializeRepositoryException extends Throwable {
    public UnableToInitializeRepositoryException(String msg) {
        super(msg);
    }

    public UnableToInitializeRepositoryException(String msg, SQLException e) {
        super(msg, e);
    }
}
