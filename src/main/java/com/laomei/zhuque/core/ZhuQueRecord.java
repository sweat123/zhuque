package com.laomei.zhuque.core;

import lombok.Data;

/**
 * Wrap value which polls from Kafka
 * @author luobo
 */
@Data
public class ZhuQueRecord {

    private Object beforeValue;

    private Object afterValue;

    public ZhuQueRecord(Object beforeValue, Object afterValue) {
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
    }
}
