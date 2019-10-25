package com.bkcc.hbase.filter;

import lombok.Data;

/**
 * 【描 述】：HBase过滤器实体类
 *
 * @author 陈汝晗
 * @version v1.0
 * @since 2019-10-24 17:01
 */
@Data
public class HBaseFilter {

    /**
     * 【描 述】：字段
     *
     *  @since 2019/10/25 08:39
     */
    private String column;

    /**
     * 【描 述】：匹配值或正则表达式
     *
     *  @since 2019/10/25 08:39
     */
    private Object value;

    /**
     * 【描 述】：是否是主键 默认否
     *
     *  @since 2019/10/25 09:05
     */
    private boolean rowKey;

    /**
     * 【描 述】：匹配规则
     *
     *  @since 2019/10/25 08:39
     */
    private HBaseFilterRule rule;

}///:~
