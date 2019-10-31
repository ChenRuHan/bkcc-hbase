package com.bkcc.hbase.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public enum HBaseFilterRule {
    LESS(CompareFilter.CompareOp.LESS),
    LESS_OR_EQUAL(CompareFilter.CompareOp.LESS_OR_EQUAL),
    EQUAL(CompareFilter.CompareOp.EQUAL),
    NOT_EQUAL(CompareFilter.CompareOp.NOT_EQUAL),
    GREATER_OR_EQUAL(CompareFilter.CompareOp.GREATER_OR_EQUAL),
    GREATER(CompareFilter.CompareOp.GREATER),
    MATCH_REGEX(CompareFilter.CompareOp.EQUAL),
    NOT_MATCH_REGEX(CompareFilter.CompareOp.NOT_EQUAL),
    ;

    private CompareFilter.CompareOp compareOp;

    public CompareFilter.CompareOp getCompareOp() {
        return this.compareOp;
    }

    public byte[] getValue(Object v) {
        byte[] value = v == null ? null : Bytes.toBytes(v.toString());
        return value;
    }

    public RegexStringComparator getRegexValue(Object v) {
        if (v == null || StringUtils.isBlank(v.toString())) {
            return null;
        }
        return new RegexStringComparator(v.toString());
    }

    HBaseFilterRule(CompareFilter.CompareOp compareOp) {
        this.compareOp = compareOp;
    }

}
