package com.ecs796p.exception;

public class MatrixException extends Exception {
    public MatrixException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
