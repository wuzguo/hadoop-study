package com.hadoop.study.spark.util

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource


object JDBCUtil {

    //初始化连接池
    var dataSource: DataSource = initPool()

    //初始化连接池方法
    def initPool(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://hadoop003:3306/spark-streaming?useUnicode=true&characterEncoding=UTF-8")
        properties.setProperty("username", "root")
        properties.setProperty("password", "123456")
        properties.setProperty("maxActive", "50")
        DruidDataSourceFactory.createDataSource(properties)
    }

    //获取MySQL连接
    def getConnection: Connection = {
        dataSource.getConnection
    }

    /**
     * 执行SQL语句,单条数据插入
     *
     * @param connection 连接
     * @param sql        SQL
     * @param params     参数
     * @return 结果
     */
    def execute(connection: Connection, sql: String, params: Array[Any]): Int = {
        var rtn = 0
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)

            if (params != null && params.length > 0) {
                for (i <- params.indices) {
                    pstmt.setObject(i + 1, params(i))
                }
            }
            rtn = pstmt.executeUpdate()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }

    /**
     * 查询
     *
     * @param connection 连接
     * @param sql        SQL
     * @param params     参数
     * @return 结果
     */
    def query(connection: Connection, sql: String, params: Array[Any]): Boolean = {
        var flag: Boolean = false
        var pstmt: PreparedStatement = null
        try {
            pstmt = connection.prepareStatement(sql)
            for (i <- params.indices) {
                pstmt.setObject(i + 1, params(i))
            }
            flag = pstmt.executeQuery().next()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        flag
    }
}
