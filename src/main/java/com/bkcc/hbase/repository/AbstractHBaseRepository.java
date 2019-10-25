package com.bkcc.hbase.repository;

import com.alibaba.fastjson.JSONObject;
import com.bkcc.hbase.annotations.HBaseColumn;
import com.bkcc.hbase.annotations.HBaseRowkey;
import com.bkcc.hbase.annotations.HBaseTable;
import com.bkcc.hbase.filter.HBaseFilter;
import com.bkcc.hbase.filter.abs.AbstractHBaseFilter;
import com.bkcc.hbase.repository.itf.HBaseCrudItf;
import com.bkcc.util.mytoken.exception.RRException;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 【描 述】：HBaseDao
 * 【环 境】：J2SE 1.8
 *
 * @author 陈汝晗
 * @version v1.0 Jun 26, 2019 新建
 * @since Jun 26, 2019
 */
@Slf4j
public abstract class AbstractHBaseRepository<T extends Serializable> extends AbstractHBaseFilter implements HBaseCrudItf<T> {

    /**
     * 【描 述】：统计表全部数据大小
     *
     * @return
     * @since Jun 27, 2019
     */
    @Override
    public long count() {
        return count(null, null);
    }

    /**
     * 【描 述】：根据rowKey范围统计表数据大小
     *
     * @param beginRowKey
     * @param endRowKey
     * @return
     * @since Jul 21, 2019
     */
    @Override
    public long count(String beginRowKey, String endRowKey) {
        return countBySacn(getScan(beginRowKey, endRowKey, null, false));
    }

    /**
     * 【描 述】：根据RowKey查询，如果查询不到返回null
     *
     * @param rowKey
     * @return T
     * @since Jun 26, 2019
     */
    @Override
    public T get(String rowKey) {
        List<T> list = list(Arrays.asList(rowKey));
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    /**
     * 【描 述】：根据多个RowKey查询，如果查询不到返回空集合
     *
     * @param rowKeyList
     * @return List<T>
     * @since Jun 26, 2019
     */
    @Override
    public List<T> list(Collection<String> rowKeyList) {
        List<T> returnList = new ArrayList<>();
        if (rowKeyList.isEmpty()) {
            return returnList;
        }
        Table table = getTable();
        try {
            List<Get> getList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            }
            Result[] results = table.get(getList);
            if (results == null || results.length == 0) {
                return returnList;
            }
            for (Result result : results) {
                T t = changeResult2T(result);
                if (t != null) {
                    returnList.add(t);
                }
            }
        } catch (IOException e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(null, table, null);
        }
        return returnList;
    }

    /**
     * 【描 述】：查询列表所有数据
     *
     * @return List<T>
     * @since Jun 26, 2019
     */
    @Override
    public List<T> list() {
        return list(null, null, null);
    }

    /**
     * 【描 述】：根据RowKey范围查询列表数据
     *
     * @param beginRowKey
     * @param endRowKey
     * @return List<T>
     * @since Jul 21, 2019
     */
    @Override
    public List<T> list(String beginRowKey, String endRowKey) {
        return listByScan(getScan(beginRowKey, endRowKey, null, false));
    }

    /**
     * 【描 述】：根据RowKey范围查询列表数据
     *
     * @param beginRowKey
     * @param endRowKey
     * @param pageSize    查询数量
     * @return List<T>
     * @since Jul 21, 2019
     */
    @Override
    public List<T> list(String beginRowKey, String endRowKey, Integer pageSize) {
        return listByScan(getScan(beginRowKey, endRowKey, pageSize, false));
    }

    /**
     * 【描 述】：根据RowKey范围查询列表数据，降序查询
     *
     * @param beginRowKey
     * @param endRowKey
     * @return List<T>
     * @since Jul 21, 2019
     */
    @Override
    public List<T> listReversed(String beginRowKey, String endRowKey) {
        return listByScan(getScan(endRowKey, beginRowKey, null, true));
    }

    /**
     * 【描 述】：根据RowKey范围查询列表数据，降序查询
     *
     * @param beginRowKey
     * @param endRowKey
     * @param pageSize    查询数量
     * @return List<T>
     * @since Jul 21, 2019
     */
    @Override
    public List<T> listReversed(String beginRowKey, String endRowKey, Integer pageSize) {
        return listByScan(getScan(endRowKey, beginRowKey, pageSize, true));
    }

    /**
     * 【描 述】：删除全部数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    @Override
    public void delete() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.truncateTable(TableName.valueOf(tableName), true);
            if (!admin.isTableEnabled(TableName.valueOf(tableName))) {
                admin.enableTable(TableName.valueOf(tableName));
            }
        } catch (IOException e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(admin, null, null);
        }
    }

    /**
     * 【描 述】：删除整条数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    @Override
    public void delete(String rowKey) {
        delete(Arrays.asList(rowKey));
    }

    /**
     * 【描 述】：批量删除整条数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    @Override
    public void delete(Collection<String> rowKeyList) {
        Table table = getTable();
        try {
            List<Delete> deletes = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                deletes.add(delete);
            }
            table.delete(deletes);
        } catch (IOException e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(null, table, null);
        }
    }

    /**
     * 【描 述】：保存/更新数据
     *
     * @param t 数据实体
     * @since Jul 24, 2019
     */
    @Override
    public void save(T t) {
        save(Arrays.asList(t));
    }

    /**
     * 【描 述】：保存/更新数据
     *
     * @param t       数据实体
     * @param columns 需要更新的列。不传为全部列
     * @since Jul 24, 2019
     */
    @Override
    public void save(T t, String... columns) {
        save(Arrays.asList(t), columns);
    }

    /**
     * 【描 述】：批量保存/更新数据
     *
     * @param tList   数据集合
     * @param columns 需要更新的列。不传为全部列
     * @since Jul 24, 2019
     */
    @Override
    public void save(Collection<T> tList, String... columns) {
        Table table = getTable();
        try {
            List<Put> putList = getPutList(tList, columns);
            if (putList == null || putList.isEmpty()) {
                return;
            }
            table.put(putList);
        } catch (IOException e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(null, table, null);
        }
    }

    /** ======================================================== 私有方法 ======================================================== */
    /**
     * 【描 述】：HBase配置
     *
     * @since Jun 26, 2019 v1.0
     */
    @Autowired
    private Configuration config;

    /**
     * 【描 述】：环境 test、dev、pro
     *
     * @since Jul 24, 2019 v1.0
     */
    @Value("${bkcc.env}")
    private String env;

    /**
     * 【描 述】：HBase链接
     *
     * @since Jun 26, 2019 v1.0
     */
    @Autowired
    private Connection connection;

    /**
     * 【描 述】：表名称
     *
     * @since Jul 1, 2019 v1.0
     */
    private String tableName;

    /**
     * 【描 述】：列族
     *
     * @since Jul 1, 2019 v1.0
     */
    private String familyColumn;

    /**
     * 【描 述】：RowKey字段名称
     *
     * @since Jul 21, 2019 v1.0
     */
    private String rowKeyField;

    /**
     * 【描 述】：范型
     *
     * @since Jul 1, 2019 v1.0
     */
    private Class<T> clazz;

    /**
     * 【描 述】：过滤器
     *
     * @since 2019/10/25 08:43
     */
    private List<HBaseFilter> filterList;

    /**
     * 【描 述】：初始化
     *
     * @throws Exception
     * @since Jul 21, 2019
     */
    @SuppressWarnings("unchecked")
    @PostConstruct
    private void init() throws Exception {
        log.debug("# 初始化hbase表信息---begin");
        Type type = getClass().getGenericSuperclass();
        if (!(type instanceof ParameterizedType)) {
            return;
        }
        ParameterizedType pType = (ParameterizedType) type;
        Type claz = pType.getActualTypeArguments()[0];
        if (claz instanceof Class) {
            this.clazz = (Class<T>) claz;
        }
        HBaseTable table = clazz.getAnnotation(HBaseTable.class);
        Field[] fields = clazz.getDeclaredFields();
        for (Field f : fields) {
            try {
                HBaseRowkey hb = f.getAnnotation(HBaseRowkey.class);
                if (hb != null) {
                    rowKeyField = f.getName();
                    break;
                }
            } catch (IllegalArgumentException e) {
                throw new RRException(e.getMessage(), e);
            }
        }
        String ns = table.nameSpace();
        if (!StringUtils.equals(env, "pro")) {
            ns = env + "_" + ns;
        }
        tableName = ns + ":" + table.tableName();
        familyColumn = table.familyColumn();
        log.debug("# tableName==>[{}], familyColnum==>[{}]", tableName, familyColumn);
        if (table.createTable()) {
            createTable();
        }
        log.debug("# 初始化hbase表信息---end");
    }


    /**
     * 【描 述】：获取Table实体
     *
     * @return
     * @throws IOException
     * @since Jul 18, 2019
     */
    private Table getTable() {
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RRException("# [" + tableName + "]表不存在：" + e.getMessage(), e);
        }
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
        if (result == null || result.isEmpty()) {
            return t;
        }
        List<Cell> cellList = result.listCells();
        JSONObject json = new JSONObject();
        String row = new String(result.getRow());
        json.put(rowKeyField, row);
        for (Cell cell : cellList) {
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
    private long countBySacn(Scan scan) {
        AggregationClient aggregationClient = null;
        Long count;
        try {
            aggregationClient = new AggregationClient(config);
            count = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(null, null, aggregationClient);
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
    private List<T> listByScan(Scan scan) {
        List<T> list = new ArrayList<>();
        Table table = getTable();
        try {
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                list.add(changeResult2T(result));
            }
        } catch (IOException e) {
            throw new RRException(e.getMessage(), e);
        } finally {
            close(null, table, null);
        }
        return list;
    }


    /**
     * 【描 述】：构造Scan
     *
     * @param beginRowKey beginRowKey
     * @param endRowKey   endRowKey
     * @param pageSize    查询数量
     * @param reversed    是否倒叙查询
     * @return
     * @since Jun 27, 2019
     */
    private Scan getScan(String beginRowKey, String endRowKey, Integer pageSize, boolean reversed) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(familyColumn));
        scan.setReversed(reversed);
        if (StringUtils.isNotBlank(beginRowKey)) {
            scan.withStartRow(Bytes.toBytes(beginRowKey));
        }
        if (StringUtils.isNotBlank(endRowKey)) {
            scan.withStopRow(Bytes.toBytes(endRowKey));
        }
        FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        super.addFilterList(fList, familyColumn);
        if (pageSize != null && pageSize > 0) {
            fList.addFilter(new PageFilter(pageSize));
        }
        scan.setFilter(fList);
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
            if (!exists) {
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
            close(admin, null, null);
        }
    }

    /**
     * 【描 述】：获取添加数据PUTList
     *
     * @param tList
     * @return
     * @since Jul 23, 2019
     */
    private List<Put> getPutList(Collection<T> tList, String... columns) {
        Set<String> colSet = new HashSet<>();
        if (columns != null && columns.length > 0) {
            colSet = new HashSet<>(Arrays.asList(columns));
        }
        List<Put> putList = new ArrayList<>();
        for (T t : tList) {
            Field[] fields = t.getClass().getDeclaredFields();
            String rowKey = "";
            Map<String, Object> map = new HashMap<>();
            for (Field f : fields) {
                try {
                    f.setAccessible(true);
                    String key = f.getName();
                    if (StringUtils.equals("serialVersionUID", key)) {
                        continue;
                    }
                    Object value = f.get(t);
                    HBaseRowkey hb = f.getAnnotation(HBaseRowkey.class);
                    if (hb != null) {
                        rowKey = value == null ? "" : value.toString();
                        continue;
                    }
                    HBaseColumn c = f.getAnnotation(HBaseColumn.class);
                    if (c == null) {
                        continue;
                    }
                    key = StringUtils.isBlank(c.value()) ? key : c.value();
                    if (colSet.isEmpty()) {
                        map.put(key, value);
                    } else {
                        if (colSet.contains(key)) {
                            map.put(key, value);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    throw new RRException(e.getMessage(), e);
                } catch (IllegalAccessException e) {
                    throw new RRException(e.getMessage(), e);
                }
            }
            if (StringUtils.isBlank(rowKey)) {
                log.warn("# RowKey为空，插入失败[{}]", t);
                continue;
            }
            Put put = new Put(Bytes.toBytes(rowKey));
            log.debug("# [{}]插入数据rowKey==>[{}],data==>[{}]", tableName, rowKey, map);
            for (String qualifier : map.keySet()) {
                Object value = map.get(qualifier);
                if (value == null) {
                    put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier), null);
                } else {
                    put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier), Bytes.toBytes(value.toString()));
                }
            }
            putList.add(put);
        }
        return putList;
    }


    /**
     * 【描 述】：关闭资源
     *
     * @param admin
     * @param table
     * @param aggregationClient
     * @return void
     * @author 陈汝晗
     * @since 2019/10/24 15:58
     */
    private void close(Admin admin, Table table, AggregationClient aggregationClient) {
        if (admin == null) {
            try {
                admin.close();
            } catch (IOException e) {
                throw new RRException(e.getMessage(), e);
            }
        }
        if (table == null) {
            try {
                table.close();
            } catch (IOException e) {
                throw new RRException(e.getMessage(), e);
            }
        }
        if (aggregationClient == null) {
            try {
                aggregationClient.close();
            } catch (IOException e) {
                throw new RRException(e.getMessage(), e);
            }
        }
    }
}///:~
