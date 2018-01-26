package com.laomei.zhuque.exception;

/**
 * @author luobo on 2018/1/26 16:16
 */
public class UnknownReducerClazzException extends Exception {

    public UnknownReducerClazzException() {}

    public UnknownReducerClazzException(String message) {
        super(message);
    }

    public UnknownReducerClazzException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownReducerClazzException(Throwable cause) {
        super(cause);
    }
}
