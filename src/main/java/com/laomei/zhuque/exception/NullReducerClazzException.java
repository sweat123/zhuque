package com.laomei.zhuque.exception;

/**
 * @author luobo on 2018/1/26 16:19
 */
public class NullReducerClazzException extends Exception {

    public NullReducerClazzException() {}

    public NullReducerClazzException(String message) {
        super(message);
    }

    public NullReducerClazzException(String message, Throwable cause) {
        super(message, cause);
    }

    public NullReducerClazzException(Throwable cause) {
        super(cause);
    }
}
