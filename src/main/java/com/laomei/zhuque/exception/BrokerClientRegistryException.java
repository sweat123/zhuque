package com.laomei.zhuque.exception;

/**
 * @author luobo on 2018/2/10 14:40
 */
public class BrokerClientRegistryException extends Exception {

    public BrokerClientRegistryException() {}

    public BrokerClientRegistryException(String message) {
        super(message);
    }

    public BrokerClientRegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    public BrokerClientRegistryException(Throwable cause) {
        super(cause);
    }
}
