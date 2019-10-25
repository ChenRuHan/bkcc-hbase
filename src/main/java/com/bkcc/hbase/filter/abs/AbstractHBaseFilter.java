package com.bkcc.hbase.filter.abs;

import com.bkcc.hbase.filter.HBaseFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 【描 述】：HBase过滤器抽象类
 *
 * @author 陈汝晗
 * @version v1.0
 * @since 2019-10-25 09:31
 */
public abstract class AbstractHBaseFilter {

    /**
     * 【描 述】：过滤器
     *
     * @since 2019/10/25 08:43
     */
    private List<HBaseFilter> filterList;

    /**
     * 【描 述】：新增过滤器
     *
     * @param filter
     * @return java.util.List<com.bkcc.hbase.filter.HBaseFilter>
     * @author 陈汝晗
     * @since 2019/10/25 09:30
     */
    public void addFilter(HBaseFilter filter) {
        addFilter(Arrays.asList(filter));
    }

    /**
     * 【描 述】：新增过滤器
     *
     * @param filters
     * @return java.util.List<com.bkcc.hbase.filter.HBaseFilter>
     * @author 陈汝晗
     * @since 2019/10/25 09:30
     */
    public void addFilter(List<HBaseFilter> filters) {
        if (filterList == null) {
            filterList = new ArrayList<>();
        }
        if (filters == null) {
            return;
        }
        filterList.addAll(filters);
    }


    /**
     * 【描 述】：构造过滤器
     *
     * @param fList
     * @param familyColumn 列族
     * @return void
     * @author 陈汝晗
     * @since 2019/10/25 09:19
     */
    protected void addFilterList(FilterList fList, String familyColumn) {
        if (fList == null || filterList == null || filterList.isEmpty()) {
            return;
        }
        byte[] fc = Bytes.toBytes(familyColumn);
        for (HBaseFilter vo : filterList) {
            if (vo == null || vo.getRule() == null) {
                continue;
            }
            if (vo.isRowKey()) {
                if (vo.getValue() == null || StringUtils.isBlank(vo.getValue().toString())) {
                    continue;
                }
                RowFilter rf = null;
                switch (vo.getRule()) {
                    case MATCH_REGEX:
                        rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(vo.getValue().toString()));
                        break;
                    case NOT_MATCH_REGEX:
                        rf = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator(vo.getValue().toString()));
                        break;
                }
                if (rf != null) {
                    fList.addFilter(rf);
                }
                continue;
            }
            if (StringUtils.isBlank(vo.getColumn())) {
                continue;
            }

            byte[] c = Bytes.toBytes(vo.getColumn());
            byte[] value = vo.getValue() == null ? null : Bytes.toBytes(vo.getValue().toString());
            SingleColumnValueFilter filter = null;
            switch (vo.getRule()) {
                case LESS:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.LESS, value));
                    break;
                case LESS_OR_EQUAL:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.LESS_OR_EQUAL, value));
                    break;
                case EQUAL:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.EQUAL, value));
                    break;
                case GREATER:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.GREATER, value));
                    break;
                case GREATER_OR_EQUAL:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.GREATER_OR_EQUAL, value));
                    break;
                case NOT_EQUAL:
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.NOT_EQUAL, value));
                    break;
                case MATCH_REGEX:
                    if (vo.getValue() == null || StringUtils.isBlank(vo.getValue().toString())) {
                        break;
                    }
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.EQUAL, new RegexStringComparator(vo.getValue().toString())));
                    break;
                case NOT_MATCH_REGEX:
                    if (vo.getValue() == null || StringUtils.isBlank(vo.getValue().toString())) {
                        break;
                    }
                    filter = (new SingleColumnValueFilter(fc, c, CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator(vo.getValue().toString())));
                    break;
            }
            if (filter != null) {
                filter.setFilterIfMissing(true);
                fList.addFilter(filter);
            }
        }
        filterList.clear();
    }

}///:~
