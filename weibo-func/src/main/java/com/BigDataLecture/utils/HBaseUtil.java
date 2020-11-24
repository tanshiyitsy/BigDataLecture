package com.BigDataLecture.utils;

import com.BigDataLecture.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/*
* 这个里面封装了一些工程的公用工具类
* 1. 创建命名空间
* 2. 判断表是否存在
* 3. 创建表（3张）
* */
public class HBaseUtil {
//    1. 创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {
//        1. 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//        2. 获取admin对象
        Admin admin = connection.getAdmin();
//        3. 构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
//         4. 创建命名空间
        admin.createNamespace(namespaceDescriptor);
//        5. 关闭资源
        admin.close();
        connection.close();
    }
//    2. 判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {
//        1. 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//        2. 获取admin对象
        Admin admin = connection.getAdmin();
//        3. 判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
//        4. 关闭资源
        admin.close();
        connection.close();
//        5. 返回结果
        return exists;
    }
//    3. 创建表
    public static void createTable(String tableName,int version, String... cfs) throws IOException {
//        1. 判断是否传入了列族信息（因为列族信息为可变形参）
        if(cfs.length == 0){
            System.out.println("请设置列族信息");
            return;
        }
//        2. 判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("表已经存在");
            return;
        }
//        3. 获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//        4. 获取admin对象
        Admin admin = connection.getAdmin();
//        5. 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//        6. 添加列族信息
        for(String cf:cfs){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //        7. 设置版本
            hColumnDescriptor.setMaxVersions(version);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
//        8. 创建表操作
        admin.createTable(hTableDescriptor);
//        9. 关闭资源
        admin.close();
        connection.close();
    }
}
