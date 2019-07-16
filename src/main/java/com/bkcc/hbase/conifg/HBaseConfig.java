package com.bkcc.hbase.conifg;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

/**
 * 【描 述】：HBase链接配置
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jun 25, 2019 新建
 *  @since          Jun 25, 2019 
 */
@Configuration
@EnableConfigurationProperties(HBaseProperties.class)
public class HBaseConfig {
	
	private final HBaseProperties properties;

    public HBaseConfig(HBaseProperties properties) {
        this.properties = properties;
    }

    @Bean
    public HbaseTemplate hbaseTemplate() {
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        hbaseTemplate.setConfiguration(configuration());
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }

    @Bean
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }
        return configuration;
    }
}
