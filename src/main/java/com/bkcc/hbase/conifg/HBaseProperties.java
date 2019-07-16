package com.bkcc.hbase.conifg;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

/**
 * 【描 述】：Hbase参数配置
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jun 25, 2019 新建
 *  @since          Jun 25, 2019 
 */
@ConfigurationProperties(prefix = "hbase")
@Data
public class HBaseProperties {

	private Map<String, String> config;
}
