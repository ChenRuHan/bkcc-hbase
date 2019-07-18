package com.bkcc.hbase.repository;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSONObject;
import com.bkcc.hbase.annotations.HBaseColumn;
import com.bkcc.hbase.annotations.HBaseRowkey;
import com.bkcc.hbase.annotations.HBaseTable;

import lombok.extern.slf4j.Slf4j;

/**
 * 【描 述】：HBaseDao
 * 【环 境】：J2SE 1.8
 *
 *  @author         陈汝晗
 *  @version        v1.0 Jun 26, 2019 新建
 *  @since          Jun 26, 2019 
 */
@Slf4j
public abstract class HBaseRepository<T extends Serializable> {
	
	/**
	 * 【描 述】：HBase配置
	 *
	 *  @since  Jun 26, 2019 v1.0
	 */
	@Autowired
	private Configuration config;

	/**
	 * 【描 述】：HBase链接
	 *
	 *  @since  Jun 26, 2019 v1.0
	 */
	@Autowired
	private Connection connection;
	
	/**
	 * 【描 述】：表名称
	 *
	 *  @since  Jul 1, 2019 v1.0
	 */
	private String tableName;
	
	/**
	 * 【描 述】：列族
	 *
	 *  @since  Jul 1, 2019 v1.0
	 */
	private String familyColumn;
	
	/**
	 * 【描 述】：范型
	 *
	 *  @since  Jul 1, 2019 v1.0
	 */
	private Class<T> clazz;
	
	@SuppressWarnings("unchecked")
	@PostConstruct
	private void init() throws Exception{
		log.debug("# 初始化hbase表信息---begin");
		Type type = getClass().getGenericSuperclass();
		if( !(type instanceof ParameterizedType) ){
			return;
		}
        ParameterizedType pType = (ParameterizedType)type;
        Type claz = pType.getActualTypeArguments()[0];
        if( claz instanceof Class ){
            this.clazz = (Class<T>) claz;
        }
		HBaseTable table = clazz.getAnnotation(HBaseTable.class);
		tableName = table.tableName();
		familyColumn = table.familyColumn();
		log.debug("# tableName==>[{}], familyColnum==>[{}]", tableName, familyColumn);
		if(table.createTable()) {
			createTable();
		}
		log.debug("# 初始化hbase表信息---end");
	}
	
	/**
	 * 【描 述】：count表
	 *
	 * @param tableName
	 * @param beginRowKey
	 * @param endRowKey
	 * @return
	 * @throws Throwable 
	 * @since Jun 27, 2019
	 */
	public long count(String beginRowKey, String endRowKey) {
		return countBySacn(getScan(beginRowKey, endRowKey, null));
	}
	
	/**
	 * 【描 述】：count表
	 *
	 * @param tableName
	 * @return
	 * @throws Throwable 
	 * @since Jun 27, 2019
	 */
	public long countAll() {
		return countBySacn(getScan(null, null, null));
	}
	
	
	
	/**
	 * 【描 述】：根据RowKey查询，如果查询不到返回null
	 *
	 * @param tableName
	 * @param rowKey
	 * @param clazz
	 * @return
	 * @throws IOException 
	 * @since Jun 26, 2019
	 */
	public T get(String rowKey){
		Result result = null;
		try {
			Table table = getTable();
			Get get = new Get(Bytes.toBytes(rowKey));
			if(get.isCheckExistenceOnly()){
				return null;
			}
			result = table.get(get);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		return changeResult2T(result);
	}
	
	/**
	 * 【描 述】：根据RowKey范围查询列表数据
	 *
	 * @param tableName
	 * @param beginRowKey
	 * @param endRowKey
	 * @param pageSize 查询数量，null或小于0代表查询全部
	 * @param clazz
	 * @return
	 * @throws IOException 
	 * @since Jun 27, 2019
	 */
	public List<T> list(String beginRowKey, String endRowKey, Long pageSize){
		return listByScan(getScan(beginRowKey, endRowKey, pageSize));
	}
	
	/**
	 * 【描 述】：查询列表所有数据
	 *
	 * @param tableName
	 * @param clazz
	 * @return
	 * @throws IOException 
	 * @since Jun 26, 2019
	 */
	public List<T> listAll() throws IOException{
		return listByScan(getScan(null, null, null));
	}
	
	/**
	 * 【描 述】：删除整条数据
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws IOException 
	 * @since Jun 26, 2019
	 */
	public void delete(String rowKey){
		try {
			Table table = getTable();
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);;
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 【描 述】：保存数据
	 *
	 * @param tableName
	 * @param rowKey
	 * @param keyValueMap 
	 * @since Jun 26, 2019
	 */
	public void save(T t) {
		Field[] fields = t.getClass().getDeclaredFields();
		String rowKey = "";
		Map<String, Object> map = new HashMap<>();
		for(Field f : fields) {
			try {
				f.setAccessible(true);
				String key = f.getName();
				if(StringUtils.equals("serialVersionUID", key)) {
					continue;
				}
				Object value = f.get(t);
				HBaseRowkey hb = f.getAnnotation(HBaseRowkey.class);
				if(hb != null) {
					rowKey = value == null ? "" : value.toString();
					continue;
				}
				HBaseColumn c = f.getAnnotation(HBaseColumn.class);
				if(c == null) {
					continue;
				}
				key = StringUtils.isBlank(c.value()) ? key : c.value();
				map.put(key, value);
			} catch (IllegalArgumentException e) {
				log.error(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				log.error(e.getMessage(), e);
			}
		}
		if(StringUtils.isBlank(rowKey)) {
			throw new NullPointerException("RowKey为空，插入失败");
		}
		try {
			
			Put put = new Put(Bytes.toBytes(rowKey));
	    	log.debug("# hbase插入数据rowKey==>[{}],data==>[{}]", rowKey, map);
			for(String qualifier : map.keySet()) {
				put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier), Bytes.toBytes(map.get(qualifier).toString())) ;
			}
			Table table = getTable();
	        table.put(put);
		} catch (IOException e1) {
			log.error(e1.getMessage(), e1);
		}
	}
	
	
	
	/** ======================================================== 私有方法 ======================================================== */
	
	/**
	 * 【描 述】：获取Table实体
	 *
	 * @return
	 * @throws IOException
	 * @since Jul 18, 2019
	 */
	private Table getTable() throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		return table;
	}
	/**
	 * 【描 述】：结果解析器
	 *
	 * @param clazz
	 * @return
	 * @since Jun 27, 2019
	 */
	private T changeResult2T(Result result) {
		T t = null;
		if(result == null || result.isEmpty()) {
			return t;
		}
		List<Cell> cellList = result.listCells();
		JSONObject json = new JSONObject();
		String row = new String(result.getRow());
		json.put("rowKey", row);
		for(Cell cell : cellList) {
			String cellName = new String(CellUtil.cloneQualifier(cell));
			String value = new String(CellUtil.cloneValue(cell));
			json.put(cellName, value);
		}
		t = JSONObject.parseObject(json.toJSONString(), clazz);
		return t;
	}
	
	
	
	/**
	 * 【描 述】：count表
	 *
	 * @param tableName
	 * @param scan
	 * @return
	 * @throws Throwable 
	 * @since Jun 27, 2019
	 */
	private long countBySacn(Scan scan){
		AggregationClient aggregationClient = null;
		Long count = 0L;
		try {
			aggregationClient = new AggregationClient(config);
			count = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				if(aggregationClient != null) {
					aggregationClient.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
		return count;
	}
	
	/**
	 * 【描 述】：根据RowKey范围查询列表数据
	 *
	 * @param tableName
	 * @param scan
	 * @param clazz
	 * @return
	 * @throws IOException 
	 * @since Jun 26, 2019
	 */
	private List<T> listByScan(Scan scan){
		List<T> list = new ArrayList<>();
		try {
			Table table = getTable();
			ResultScanner results = table.getScanner(scan);
			for (Result result : results){
				list.add(changeResult2T(result));
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		return list;
	}
	

	/**
	 * 【描 述】：构造Scan
	 *
	 * @param beginRowKey
	 * @param endRowKey
	 * @param pageSize
	 * @return
	 * @since Jun 27, 2019
	 */
	private Scan getScan(String beginRowKey, String endRowKey, Long pageSize) {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(familyColumn));
		if(StringUtils.isNotBlank(beginRowKey)) {
			scan.withStartRow(Bytes.toBytes(beginRowKey));
		}
		if(StringUtils.isNotBlank(endRowKey)) {
			scan.withStopRow(Bytes.toBytes(endRowKey));
		}
		if(pageSize != null && pageSize > 0) {
			scan.setFilter(new PageFilter(pageSize));
		}
		return scan;
	}
	
	/**
	 * 【描 述】：创建表
	 *
	 * @param tableName
	 * @throws IOException 
	 * @since Jun 26, 2019
	 */
	private void createTable() throws IOException {
		log.debug("# hbase 开始创建表:[{}]", tableName);
        Admin admin = null;
        try {
			admin = connection.getAdmin();
			boolean exists = admin.tableExists(TableName.valueOf(tableName));
			if(!exists) {
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            	tableDesc.addFamily(new HColumnDescriptor(familyColumn));
                admin.createTable(tableDesc);
                log.debug("# [{}]创建成功", tableName);
			} else {
				log.debug("# [{}]已经存在不需要创建", tableName);
			}
		} catch (IOException e) {
			throw e;
		} finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                	throw e;
                }
            }
		}
	}
	
	
}///:~
