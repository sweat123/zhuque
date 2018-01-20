package com.laomei.zhuque.exception;

/**
 * this exception will be throwed if the result of SQL if null when we are required;
 * @author luobo
 **/
public class NoResultException extends Exception {
    public NoResultException() {}

    public NoResultException(String message) {
        super(message);
    }

    public NoResultException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoResultException(Throwable cause) {
        super(cause);
    }
}
