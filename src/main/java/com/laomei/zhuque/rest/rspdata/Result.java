package com.laomei.zhuque.rest.rspdata;

import lombok.Data;
import org.springframework.http.HttpStatus;

/**
 * @author luobo
 */
@Data
public class Result<T> {
    private int state;

    private T body;

    public Result() {}

    public Result(T body) {
        this.body = body;
    }

    public Result(int state, T body) {
        this.state = state;
        this.body = body;
    }

    public static <K> Result<K> ok(K body) {
        return new Result<>(HttpStatus.OK.value(), body);
    }

    public static <K> Result<K> notFount(K body) {
        return new Result<>(HttpStatus.NOT_FOUND.value(), body);
    }

    public static <K> Result<K> badRequest(K body) {
        return new Result<>(HttpStatus.BAD_REQUEST.value(), body);
    }

    public static <K> Result<K> serverError(K body) {
        return new Result<>(HttpStatus.INTERNAL_SERVER_ERROR.value(), body);
    }
}
