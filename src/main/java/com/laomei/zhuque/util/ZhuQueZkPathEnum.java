package com.laomei.zhuque.util;

/**
 * @author luobo
 **/
public enum ZhuQueZkPathEnum {

    ROOTPATH("/zhuque"),
    TASK_ROOT_PATH("/zhuque/tasks");

    private String path;

    ZhuQueZkPathEnum(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
