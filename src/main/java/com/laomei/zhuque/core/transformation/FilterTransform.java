package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.contants.ZhuQueContants;
import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentPreProcessor.FilterTrans;
import com.laomei.zhuque.util.ObjTypeUtil;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
public class FilterTransform implements Transform {

    private FilterTrans filterTrans;

    public FilterTransform(FilterTrans filterTrans) {
        Preconditions.checkNotNull(filterTrans);
        this.filterTrans = filterTrans;
    }

    @Override
    public Map<String, Object> transform(final Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        return doFilterWithContext(context);
    }

    /**
     * If the value in context is not same in FilterTrans, then return null;
     * @return If values in context is same in FilterTrans, return context; If not return false;
     */
    private Map<String, Object> doFilterWithContext(Map<String, Object> context) {
        if (filterTrans.getExist() != null && !doExistFilter(context, filterTrans.getExist())) {
            return null;
        }
        if (filterTrans.getNotExist() != null && !doNotExistFilter(context, filterTrans.getNotExist())) {
            return null;
        }
        if (filterTrans.getMatch() != null && !doMatchFilter(context, filterTrans.getMatch())) {
            return null;
        }
        if (filterTrans.getNotMatch() != null && !doNotMatchFilter(context, filterTrans.getNotMatch())) {
            return null;
        }
        if (filterTrans.getIn() != null && !doInFilter(context, filterTrans.getIn())) {
            return null;
        }
        if (filterTrans.getNotIn() != null && !doNotInFilter(context, filterTrans.getNotIn())) {
            return null;
        }
        if (filterTrans.getRange() != null && !doRangeFilter(context, filterTrans.getRange())) {
            return null;
        }
        return context;
    }

    private boolean doRangeFilter(Map<String, Object> context, Map<String, Map<String, String>> rangeFilter) {
        for (Map.Entry<String, Map<String, String>> entry : rangeFilter.entrySet()) {
            String field = entry.getKey();
            Map<String, String> ranges = entry.getValue();
            Object contextValue = context.get(field);
            if (contextValue == null) {
                return false;
            }
            if (!contextValueInRange(contextValue, ranges)) {
                return false;
            }
        }
        return true;
    }

    private boolean contextValueInRange(Object value, Map<String, String> range) {
        String gtv = range.get(ZhuQueContants.GT);
        String gtev = range.get(ZhuQueContants.GTE);
        String ltv = range.get(ZhuQueContants.LT);
        String ltev = range.get(ZhuQueContants.LTE);
        if (null != gtv) {
            return ObjTypeUtil.compare(value, gtv) > 0;
        }
        if (null != gtev) {
            int v = ObjTypeUtil.compare(value, gtev);
            return v > 0 || v == 0;
        }
        if (null != ltv) {
            return ObjTypeUtil.compare(value, ltv) < 0;
        }
        if (null != ltev) {
            int v = ObjTypeUtil.compare(value, ltev);
            return v < 0 || v == 0;
        }
        return true;
    }

    /**
     * verify the value from context is not in values from inFilter
     * @param context context
     * @param notInFilter value set of each fields;
     * @return If the value from context is not in values from inFilter, return true;
     */
    private boolean doNotInFilter(Map<String, Object> context, Map<String, List<String>> notInFilter) {
        for (Map.Entry<String, List<String>> entry : notInFilter.entrySet()) {
            String field = entry.getKey();
            List<String> allValues = entry.getValue();
            Object contextValue = context.get(field);
            for (String value : allValues) {
                if (ObjTypeUtil.equals(contextValue, value)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * verify the value from context is in values from inFilter
     * @param context context
     * @param inFilter value set of each fields;
     * @return If the value from context is in values from inFilter, return true;
     */
    private boolean doInFilter(Map<String, Object> context, Map<String, List<String>> inFilter) {
        for (Map.Entry<String, List<String>> entry : inFilter.entrySet()) {
            String field = entry.getKey();
            List<String> allValues = entry.getValue();
            Object contextValue = context.get(field);
            if (!contextValueInAllValues(contextValue, allValues)) {
                return false;
            }
        }
        return true;
    }

    private boolean contextValueInAllValues(Object contextValue, List<String> allValues) {
        for (String value : allValues) {
            if (ObjTypeUtil.equals(contextValue, value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * verify the value of fields from matchFilter is not same in context
     * @param context context
     * @param notMatchFilter the fields and value need to verify
     * @return If all the value of fields is not same in context, return true;
     */
    private boolean doNotMatchFilter(Map<String, Object> context, Map<String, String> notMatchFilter) {
        for (Map.Entry<String, String> notMatchEntry : notMatchFilter.entrySet()) {
            String field = notMatchEntry.getKey();
            String expectValue = notMatchEntry.getValue();
            Object contextValue = context.get(field);
            if (ObjTypeUtil.equals(contextValue, expectValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * verify the value of fields from matchFilter is same in context
     * @param context context
     * @param matchFilter the fields and value need to verify
     * @return If all the value of fields is same in context, return true;
     */
    private boolean doMatchFilter(Map<String, Object> context, Map<String, String> matchFilter) {
        for (Map.Entry<String, String> matchEntry : matchFilter.entrySet()) {
            String field = matchEntry.getKey();
            String expectValue = matchEntry.getValue();
            Object contextValue = context.get(field);
            if (!ObjTypeUtil.equals(contextValue, expectValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * verify the value of fields from existFilter is not existed in context;
     * @param context context
     * @param notExistFilter the fields need to verify
     * @return If all the value of fields is not existed in context, return true;
     */
    private boolean doNotExistFilter(Map<String, Object> context, List<String> notExistFilter) {
        for (String field : notExistFilter) {
            if (context.get(field) != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * verify the value of fields from existFilter is existed in context;
     * @param context context
     * @param existFilter the fields need to verify
     * @return If all the value of fields is existed in context, return true;
     */
    private boolean doExistFilter(Map<String, Object> context, List<String> existFilter) {
        for (String field : existFilter) {
            if (context.get(field) == null) {
                return false;
            }
        }
        return true;
    }
}
