package com.laomei.zhuque.exception;

/**
 * get schema failed in SchemaHelper
 * @author luobo
 **/
public class InitSchemaFailedException extends Exception {

    public InitSchemaFailedException() {}

    public InitSchemaFailedException(String message) {
        super(message);
    }

    public InitSchemaFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitSchemaFailedException(Throwable cause) {
        super(cause);
    }
}
