/*
 * SyncAssignmentVo.java
 * Copyright 2018 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.laomei.zhuque.rest.rspdata;

import com.laomei.zhuque.core.SyncAssignment;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author luobo
 */
@Data
@AllArgsConstructor
public class SyncAssignmentVo {

    private String name;

    private SyncAssignment assignment;
}
