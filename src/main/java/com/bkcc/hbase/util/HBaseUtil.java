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
	 * 【描 述】：获取升序排列的时间戳，长度为13位
	 *
	 * @return
	 * @since Jun 26, 2019
	 */
	public static String getAscCurrent() {
		return fillKey((9999999999999L - System.currentTimeMillis()), 13);
	}
	
	/**
	 * 【描 述】：获取升序排列id（max - id），保持位数一致
	 *
	 * @param key
	 * @return
	 * @since Jun 26, 2019
	 */
	public static String getAscId(Long max, Long id) {
		return fillKey(max - id, (max+"").length());
	}
	
	/**
	 * 【描 述】：获取升序排列id（MY_MAX_VALUE-id），保持位数为9位
	 *
	 * @param key
	 * @return
	 * @since Jun 26, 2019
	 */
	public static String getAscId(Long id) {
		return getAscId(MY_MAX_VALUE, id);
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
	
	public final static Long MY_MAX_VALUE = 999999999L;
	
	private final static Integer MY_MAX_LENGTH = 9;
	
	private HBaseUtil() {}
}///:~
