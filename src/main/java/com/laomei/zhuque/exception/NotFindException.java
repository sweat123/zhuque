/*
 * NotFindException.java
 * Copyright 2018 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.laomei.zhuque.exception;

/**
 * @author luobo
 */
public class NotFindException extends Exception {
    public NotFindException() {}

    public NotFindException(String message) {
        super(message);
    }

    public NotFindException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotFindException(Throwable cause) {
        super(cause);
    }
}
