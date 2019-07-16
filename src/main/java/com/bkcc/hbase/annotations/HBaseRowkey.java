package com.bkcc.hbase.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 【描 述】：HBase Rowkey标志
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jul 1, 2019 新建
 *  @since          Jul 1, 2019 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
@Inherited
public @interface HBaseRowkey {

}///:~
