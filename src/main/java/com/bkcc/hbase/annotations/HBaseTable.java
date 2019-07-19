package com.bkcc.hbase.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Persistent;

/**
 * 【描 述】：HBase实体类注解
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jul 1, 2019 新建
 *  @since          Jul 1, 2019 
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface HBaseTable {
	
	/**
	 * 【描 述】：HBase表名称
	 *
	 * @return
	 * @since Jul 1, 2019
	 */
	String tableName();
	
	/**
	 * 【描 述】：命名空间，默认为default，可以自己根据业务逻辑自己设置，命名空间需自己创建
	 *
	 * @return
	 * @since Jul 19, 2019
	 */
	String nameSpace() default "default";
	
	/**
	 * 【描 述】：列族名称 默认fc
	 *
	 * @return
	 * @since Jul 1, 2019
	 */
	String familyColumn() default "fc";
	
	/**
	 * 【描 述】：检查是否创建表
	 *
	 * @return
	 * @since Jul 1, 2019
	 */
	boolean createTable() default false;

}///：～
