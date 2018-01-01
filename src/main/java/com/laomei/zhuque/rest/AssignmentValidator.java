package com.laomei.zhuque.rest;

import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.NotValidationException;

/**
 * @author luobo
 */
public class AssignmentValidator {

    private volatile static AssignmentValidator validator;

    public static AssignmentValidator getValidator() {
        if (validator == null) {
            synchronized (AssignmentValidator.class) {
                if (validator == null) {
                    validator = new AssignmentValidator();
                }
            }
        }
        return validator;
    }

    private AssignmentValidator() {}

    public boolean isValid(String configuration) throws NotValidationException {
        try {
            SyncAssignment.newSyncTaskMetadata(configuration);
            return true;
        } catch (Exception ignore) {
            throw new NotValidationException("please check your sync configuration;", ignore);
        }
    }
}
