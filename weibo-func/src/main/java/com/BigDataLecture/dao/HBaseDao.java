package com.BigDataLecture.dao;

import com.BigDataLecture.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/*
* 业务级别的代码
* 1. 发布微博
* 2. 删除微博
* 3. 关注用户
* 4. 取关用户
* 5. 获取用户微博详情
* 6. 获取用户的初始化页面
* */
public class HBaseDao {
//    1. 发布微博
    public static void publishWeiBo(String uid,String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//        1.1 更新微博内容表
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
//        1.2 获取时间戳
        long ts = System.currentTimeMillis();
//        1.3 获取RowKey
        String rowKey = uid + "_" + ts;
//        1.4 创建put对象并赋值
        Put conPut = new Put(Bytes.toBytes(rowKey));
//        第一个参数是列族，第二个参数是列，第三个参数是value
        conPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));
//        执行插入数据操作
        contTable.put(conPut);

//        2. 更新初始化页面表
//        可以批量给所有的粉丝用户更新
//        2.1 获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
//        2.2 获取当前发布微博人的fans列族
         Get get = new Get(Bytes.toBytes(uid)); // 这里得到的是两个列族，我们只需要粉丝数据，因此需要指定一下列族
         get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
         Result result = relaTable.get(get);
//         2.3 创建一个集合，用户存放微博内容表的put对象
        ArrayList<Put> inboxPuts = new ArrayList<Put>();
//        2.4 遍历该用户的粉丝，加到list里面，实现批量更新
         for(Cell cell : result.rawCells()){
//             2.5 构建微博收件箱表的Put对象,这里只得到了列
             Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
//          2.6 给收件箱表的Put对象赋值
             inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
             inboxPuts.add(inboxPut);
         }

//         2.7 判断是否有粉丝,有粉丝才需要更新收件箱表
        if(inboxPuts.size() > 0){
//            获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
//            执行收件箱标数据插入操作
            inboxTable.put(inboxPuts);
//            关闭收件箱表
            inboxTable.close();
        }
//        关闭资源
        relaTable.close();
        contTable.close();
        connection.close();
    }

//    2. 关注用户
//    假设A关注BCD，
//    需要在relation表里面增加A的关注BCD，同时修改BCD的粉丝，增加A
//    修改初始化页面表的A，增加BCD的最新微博动态
//    需要从content表里获取微博的rowKey
//    uid为A，后面的可变形参为A想要关注的人
    public static void addAttends(String uid,String... attends) throws IOException {
        if(attends.length <= 0){
            System.out.println("请选择待关注的人");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//        1. 2.1 更新用户关系表
//        1. 获取用户关系表对象

//        2. 创建一个集合，用于存放用户关系表的put对象

//        3. 创建操作者（A） 的put对象

//        4. 循环创建被关注人的Put对象
    }
}
