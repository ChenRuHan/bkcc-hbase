package com.bkcc.hbase.util;

/**
 * 【描 述】：HBase工具类
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jun 26, 2019 新建
 *  @since          Jun 26, 2019 
 */
public class HBaseUtil {

	/**
	 * 【描 述】：获取时间戳反转值，长度为7位
	 *
	 * @return
	 * @since Jun 26, 2019
	 */
	public static Long getReverseCurrent() {
		return (HBaseUtil.MY_MAX_VALUE - System.currentTimeMillis());
	}
	
	/**
	 * 【描 述】：填充ID到9位，左侧补0
	 *
	 * @param key
	 * @return
	 * @since Jun 26, 2019
	 */
	public static String fillKey(Object key) {
		return fillKey(key, MY_MAX_LENGTH);
	}
	
	/**
	 * 【描 述】：填充ID到指定位数，左侧补0
	 *
	 * @param key key
	 * @param length 指定位数
	 * @return
	 * @since Jun 26, 2019
	 */
	public static String fillKey(Object key, Integer length) {
		if(key == null) {
			key = "";
		}
		int len = length - key.toString().length();
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < len; i++) {
			sb.append("0");
		}
		sb.append(key);
		return sb.toString();
	}
	
	private final static Long MY_MAX_VALUE = 9999999999999L;
	
	private final static Integer MY_MAX_LENGTH = 9;
	
	private HBaseUtil() {}
}///:~
