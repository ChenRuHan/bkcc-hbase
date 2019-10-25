package com.bkcc.hbase.filter.itf;

import com.bkcc.hbase.filter.HBaseFilter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;

/**
 * 【描 述】：Hbase过滤器接口
 *
 * @author 陈汝晗
 * @version v1.0
 * @since 2019-10-25 09:22
 */
public interface HBaseFilterItf {

    /**
     * 【描 述】：新增过滤器
     *
     * @param filter
     * @return java.util.List<com.bkcc.hbase.filter.HBaseFilter>
     * @author 陈汝晗
     * @since 2019/10/25 09:30
     */
    List<HBaseFilter> addFilter(HBaseFilter filter);

    /**
     * 【描 述】：新增过滤器
     *
     * @param filters
     * @return java.util.List<com.bkcc.hbase.filter.HBaseFilter>
     * @author 陈汝晗
     * @since 2019/10/25 09:30
     */
    List<HBaseFilter> addFilter(List<HBaseFilter> filters);

    /**
     * 【描 述】：构建过滤器
     *
     * @param fList
     * @param familyColumn
     * @return void
     * @author 陈汝晗
     * @since 2019/10/25 09:37
     */
    void addFilterList(FilterList fList, String familyColumn);

}///:~
