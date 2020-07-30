package com.bkcc.hbase.conifg;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

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

    /*

# hbase配置信息
# 生产环境：hb-2zejh008601i8n321-001.hbase.rds.aliyuncs.com
# 研发环境：master,slave01,slave02
hbase:
  config:
    hbase.zookeeper.quorum: master,slave01,slave02
    hbase.zookeeper.property.clientPort: 2181


     */
	private Map<String, String> config;
}
