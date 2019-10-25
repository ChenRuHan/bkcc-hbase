package com.bkcc.hbase.repository.itf;


import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * 【描 述】：Hbase增删改查接口
 *
 * @author 陈汝晗
 * @version v1.0
 * @since 2019-10-25 09:25
 */
public interface HBaseCrudItf<T extends Serializable> {

    /**
     * 【描 述】：统计表全部数据大小
     *
     * @return
     * @since Jun 27, 2019
     */
    long count();

    /**
     * 【描 述】：根据rowKey范围统计表数据大小
     *
     * @param beginRowKey
     * @param endRowKey
     * @return
     * @since Jul 21, 2019
     */
    long count(String beginRowKey, String endRowKey);

    /**
     * 【描 述】：根据RowKey查询，如果查询不到返回null
     *
     * @param rowKey
     * @return T
     * @since Jun 26, 2019
     */
    T get(String rowKey);

    /**
     * 【描 述】：根据多个RowKey查询，如果查询不到返回空集合
     *
     * @param rowKeyList
     * @return List<T>
     * @since Jun 26, 2019
     */
    List<T> list(Collection<String> rowKeyList);

    /**
     * 【描 述】：查询列表所有数据
     *
     * @return List<T>
     * @since Jun 26, 2019
     */
    List<T> list();

    /**
     * 【描 述】：根据RowKey范围查询列表数据
     *
     * @param beginRowKey
     * @param endRowKey
     * @return List<T>
     * @since Jul 21, 2019
     */
    List<T> list(String beginRowKey, String endRowKey);

    /**
     * 【描 述】：根据RowKey范围查询列表数据
     *
     * @param beginRowKey
     * @param endRowKey
     * @param pageSize    查询数量
     * @return List<T>
     * @since Jul 21, 2019
     */
    List<T> list(String beginRowKey, String endRowKey, Integer pageSize);

    /**
     * 【描 述】：根据RowKey范围查询列表数据，降序查询
     *
     * @param beginRowKey
     * @param endRowKey
     * @return List<T>
     * @since Jul 21, 2019
     */
    List<T> listReversed(String beginRowKey, String endRowKey);

    /**
     * 【描 述】：根据RowKey范围查询列表数据，降序查询
     *
     * @param beginRowKey
     * @param endRowKey
     * @param pageSize    查询数量
     * @return List<T>
     * @since Jul 21, 2019
     */
    List<T> listReversed(String beginRowKey, String endRowKey, Integer pageSize);

    /**
     * 【描 述】：删除全部数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    void delete();

    /**
     * 【描 述】：删除整条数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    void delete(String rowKey);

    /**
     * 【描 述】：批量删除整条数据
     *
     * @param rowKey
     * @since Jun 26, 2019
     */
    void delete(Collection<String> rowKeyList);

    /**
     * 【描 述】：保存/更新数据
     *
     * @param t 数据实体
     * @since Jul 24, 2019
     */
    void save(T t);

    /**
     * 【描 述】：保存/更新数据
     *
     * @param t       数据实体
     * @param columns 需要更新的列。不传为全部列
     * @since Jul 24, 2019
     */
    void save(T t, String... columns);

    /**
     * 【描 述】：批量保存/更新数据
     *
     * @param tList   数据集合
     * @param columns 需要更新的列。不传为全部列
     * @since Jul 24, 2019
     */
    void save(Collection<T> tList, String... columns);

}///:~
